//! `prod-synth verify MANIFEST` — offline structural, digest, footer, and census
//! verification.
//!
//! It contacts nothing. That is the point: a fixture is generated on one machine
//! and benchmarked on another, and the receiving machine must be able to prove that
//! what it has is what was made, before it loads a byte of it into anything.
//!
//! What is checked, and what each one catches:
//!
//! * **manifest/topology version + disclaimer** — a file from another format, or a
//!   file that will not admit to being synthetic.
//! * **file digests** — truncation and corruption in transit.
//! * **Parquet footers** — that the file's own recorded key/time bounds match the
//!   manifest's. This is the one that catches a generator bug rather than a transport
//!   bug: if the writer's idea of a part's key range ever drifted from the graph's,
//!   the catalog would prune against a range the file does not have, and rows would
//!   vanish from query results with nothing in any log.
//! * **census** — manifest rows = topology rows = footer rows.
//! * **membership** — every part's declared members are inside its declared range,
//!   and the range brackets them exactly.

use std::collections::BTreeMap;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use prod_synth_contract::{Manifest, Topology};

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Contract(#[from] prod_synth_contract::ContractError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("{0}")]
    Mismatch(String),
}

type Result<T> = std::result::Result<T, VerifyError>;

#[derive(Debug)]
pub struct Verified {
    pub manifest: Manifest,
    pub topology: Topology,
    pub parts: usize,
    pub rows: u64,
    pub bytes: u64,
}

pub fn verify(manifest_path: &Path) -> Result<Verified> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));

    let manifest_bytes = read(manifest_path)?;
    let manifest = Manifest::parse(&manifest_path.display().to_string(), &manifest_bytes)?;

    // The topology, by the digest the manifest recorded. A topology from another
    // seed is a digest error here, not a subtly wrong benchmark three hours later.
    let topology_path = dir.join(&manifest.topology.path);
    let topology_bytes = read(&topology_path)?;
    manifest
        .topology
        .verify(&topology_path.display().to_string(), &topology_bytes)?;
    let topology = Topology::parse(&topology_path.display().to_string(), &topology_bytes)?;

    if topology.config != manifest.config {
        return Err(VerifyError::Mismatch(
            "manifest and topology were generated under different configurations".into(),
        ));
    }

    // The topology's own view of how many rows each part holds.
    let mut topo_rows: BTreeMap<u32, u64> = BTreeMap::new();
    let mut topo_members: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
    for s in &topology.shards {
        for m in &s.memberships {
            if m.tenants.len() != m.rows.len() {
                return Err(VerifyError::Mismatch(format!(
                    "part {}: {} members but {} row counts",
                    m.part,
                    m.tenants.len(),
                    m.rows.len()
                )));
            }
            if m.rows.contains(&0) {
                return Err(VerifyError::Mismatch(format!(
                    "part {}: a member has zero rows, so the graph the manifest claims is not the \
                     graph the files hold",
                    m.part
                )));
            }
            topo_rows.insert(m.part, m.rows.iter().sum());
            topo_members.insert(m.part, m.tenants.clone());
        }
    }

    let mut rows = 0u64;
    let mut bytes = 0u64;

    for part in &manifest.parts {
        let path = dir.join(&part.path);
        let raw = read(&path)?;

        let actual = prod_synth_contract::digest_bytes(&raw);
        if actual != part.digest {
            return Err(VerifyError::Contract(
                prod_synth_contract::ContractError::Digest {
                    path: path.display().to_string(),
                    expected: part.digest.clone(),
                    actual,
                },
            ));
        }
        if raw.len() as u64 != part.bytes {
            return Err(VerifyError::Mismatch(format!(
                "{}: manifest says {} bytes, file is {}",
                part.path,
                part.bytes,
                raw.len()
            )));
        }

        // Membership truth: the declared range must bracket every declared member,
        // and must be exactly their min and max — not merely contain them. A range
        // wider than its members would prune correctly but over-fetch, and would
        // quietly weaken the very measurement this fixture exists to make.
        let members = topo_members.get(&part.index).ok_or_else(|| {
            VerifyError::Mismatch(format!(
                "part {} is in the manifest but not the topology",
                part.index
            ))
        })?;
        if members.len() as u64 != part.distinct_keys {
            return Err(VerifyError::Mismatch(format!(
                "part {}: manifest declares {} distinct keys, topology holds {}",
                part.index,
                part.distinct_keys,
                members.len()
            )));
        }
        let (lo, hi) = (
            *members.iter().min().expect("non-empty"),
            *members.iter().max().expect("non-empty"),
        );
        if lo != part.key_min || hi != part.key_max {
            return Err(VerifyError::Mismatch(format!(
                "part {}: declared key range [{}, {}] is not its members' range [{lo}, {hi}]",
                part.index, part.key_min, part.key_max
            )));
        }

        // The footer: what the file itself says it holds.
        let reader = SerializedFileReader::new(bytes::Bytes::from(raw.clone()))?;
        let meta = reader.metadata();
        let file_rows: i64 = meta.file_metadata().num_rows();
        if file_rows as u64 != part.rows {
            return Err(VerifyError::Mismatch(format!(
                "{}: manifest says {} rows, the Parquet footer says {file_rows}",
                part.path, part.rows
            )));
        }
        if topo_rows.get(&part.index) != Some(&part.rows) {
            return Err(VerifyError::Mismatch(format!(
                "part {}: manifest says {} rows, the topology's memberships sum to {:?}",
                part.index,
                part.rows,
                topo_rows.get(&part.index)
            )));
        }

        let (kmin, kmax) = footer_int64_bounds(&reader, "team_id")?;
        if kmin != part.key_min || kmax != part.key_max {
            return Err(VerifyError::Mismatch(format!(
                "{}: the file's team_id bounds are [{kmin}, {kmax}] but the manifest declares \
                 [{}, {}] — the catalog would prune against a range the file does not have",
                part.path, part.key_min, part.key_max
            )));
        }
        let (tmin, tmax) = footer_int64_bounds(&reader, "timestamp")?;
        if tmin < part.ts_min || tmax > part.ts_max {
            return Err(VerifyError::Mismatch(format!(
                "{}: the file's timestamps [{tmin}, {tmax}] escape the declared span [{}, {}]",
                part.path, part.ts_min, part.ts_max
            )));
        }

        rows += part.rows;
        bytes += part.bytes;
    }

    if rows != manifest.generated.rows {
        return Err(VerifyError::Mismatch(format!(
            "the parts hold {rows} rows; the manifest's summary says {}",
            manifest.generated.rows
        )));
    }
    if bytes != manifest.generated.bytes {
        return Err(VerifyError::Mismatch(format!(
            "the parts are {bytes} bytes; the manifest's summary says {}",
            manifest.generated.bytes
        )));
    }

    let parts = manifest.parts.len();
    Ok(Verified {
        manifest,
        topology,
        parts,
        rows,
        bytes,
    })
}

/// Min/max of an Int64 column across every row group, from the footer alone. No
/// row is read: the footer is what the catalog and the scan planner will trust, so
/// the footer is what has to be true.
fn footer_int64_bounds(
    reader: &SerializedFileReader<bytes::Bytes>,
    column: &str,
) -> Result<(i64, i64)> {
    let meta = reader.metadata();
    let mut lo = i64::MAX;
    let mut hi = i64::MIN;
    for rg in meta.row_groups() {
        let col = rg
            .columns()
            .iter()
            .find(|c| c.column_path().string() == column)
            .ok_or_else(|| VerifyError::Mismatch(format!("no column '{column}' in the footer")))?;
        let Some(Statistics::Int64(s)) = col.statistics() else {
            return Err(VerifyError::Mismatch(format!(
                "column '{column}' has no Int64 statistics in the footer"
            )));
        };
        let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
            return Err(VerifyError::Mismatch(format!(
                "column '{column}' has no min/max in the footer"
            )));
        };
        lo = lo.min(*min);
        hi = hi.max(*max);
    }
    if lo > hi {
        return Err(VerifyError::Mismatch(format!(
            "column '{column}' has no row groups"
        )));
    }
    Ok((lo, hi))
}

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|source| VerifyError::Io {
        path: path.display().to_string(),
        source,
    })
}
