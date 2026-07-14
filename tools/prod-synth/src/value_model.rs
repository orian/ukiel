//! The synthetic column values — the part of the fixture the profile says
//! **nothing** about.
//!
//! The capture is geometry: which tenants live in which parts, how sparse a key
//! range is, how skewed tenant activity is. It contains no event names, no URLs,
//! no property cardinalities, no person identifiers, no per-column compression.
//! So none of that can be *derived*, and pretending otherwise would be the exact
//! failure plan 45 exists to avoid: a fixture that looks like production and
//! answers a question about something else.
//!
//! Everything here is therefore **declared**: a fixed, versioned, deliberately
//! plausible vocabulary, stored verbatim in the manifest so a reader can see at a
//! glance which columns are evidence and which are invention. It is useful query
//! data — skewed enough that `top events` and `count distinct persons` are real
//! work — and it is a claim about nothing.
//!
//! The one structural constraint that *is* real: `properties` is valid compact
//! JSON text whose promoted values exactly equal the three `mat_*` columns, because
//! that is the physical reality of the captured ClickHouse `String` column and its
//! materialized projections. A query that reads `mat_$lib` and a query that digs the
//! same key out of the JSON must agree, or the fixture cannot be used to compare
//! them.

use prod_synth_contract::{ValueModel, WeightedValue};

use crate::rng::StableRng;

pub const VALUE_MODEL_VERSION: &str = "prod-synth-values/v1";

fn weighted(pairs: &[(&str, f64)]) -> Vec<WeightedValue> {
    pairs
        .iter()
        .map(|(v, w)| WeightedValue {
            value: (*v).to_string(),
            weight: *w,
        })
        .collect()
}

/// The versioned default. Skewed, because a uniform vocabulary makes `top events`
/// a tie and `GROUP BY` a formality — and production event streams are anything
/// but uniform.
pub fn default_model() -> ValueModel {
    ValueModel {
        version: VALUE_MODEL_VERSION.to_string(),
        events: weighted(&[
            ("$pageview", 45.0),
            ("$autocapture", 28.0),
            ("$pageleave", 12.0),
            ("$identify", 5.0),
            ("$set", 3.0),
            ("$groupidentify", 1.5),
            ("feature_flag_called", 2.0),
            ("survey shown", 0.8),
            ("recording_started", 1.2),
            ("custom_event", 1.5),
        ]),
        urls: weighted(&[
            ("/", 30.0),
            ("/pricing", 12.0),
            ("/docs", 11.0),
            ("/blog", 9.0),
            ("/signup", 7.5),
            ("/login", 7.0),
            ("/dashboard", 6.0),
            ("/settings", 4.0),
            ("/docs/api", 3.5),
            ("/product", 3.0),
            ("/about", 2.5),
            ("/contact", 2.0),
            ("/careers", 1.5),
            ("/changelog", 1.0),
        ]),
        hosts: weighted(&[
            ("app.example.com", 55.0),
            ("www.example.com", 25.0),
            ("docs.example.com", 12.0),
            ("blog.example.com", 5.0),
            ("staging.example.com", 3.0),
        ]),
        libs: weighted(&[
            ("web", 62.0),
            ("posthog-js", 18.0),
            ("posthog-python", 8.0),
            ("posthog-node", 6.0),
            ("posthog-android", 3.0),
            ("posthog-ios", 3.0),
        ]),
        persons_per_tenant_min: 1,
        persons_per_tenant_max: 5_000,
        person_zipf_exponent: 1.1,
    }
}

/// One generated row's synthetic values.
pub struct Row {
    pub event: String,
    pub distinct_id: String,
    pub uuid: String,
    pub properties: String,
    pub elements_chain: String,
    pub current_url: String,
    pub host: String,
    pub lib: String,
}

/// Draws rows for one tenant. Built once per (part, tenant) so the person
/// population is stable within a tenant — repeated `distinct_id`s are the whole
/// point of the `count distinct persons` query, and a fresh person per row would
/// make it a row count in disguise.
pub struct RowGen<'a> {
    model: &'a ValueModel,
    event_w: Vec<f64>,
    url_w: Vec<f64>,
    host_w: Vec<f64>,
    lib_w: Vec<f64>,
    /// Person indices for this tenant, with Zipf-like repeat weights.
    persons: Vec<u64>,
    person_w: Vec<f64>,
    tenant: i64,
    rng: StableRng,
}

impl<'a> RowGen<'a> {
    pub fn new(model: &'a ValueModel, tenant: i64, tenant_rows: u64, seed: u64) -> Self {
        let rng = StableRng::stream(seed, &format!("rows-{tenant}"));

        // Persons scale with activity, bounded: a one-row tenant has one person, a
        // millions-of-rows tenant does not have millions.
        let want = (tenant_rows as f64).sqrt().ceil() as u64;
        let n = want.clamp(model.persons_per_tenant_min, model.persons_per_tenant_max);

        let persons: Vec<u64> = (0..n).collect();
        // Zipf-like: rank r gets weight 1/r^s. A handful of persons produce most of
        // the events, which is what a real product's activity looks like.
        let person_w: Vec<f64> = (1..=n)
            .map(|r| 1.0 / (r as f64).powf(model.person_zipf_exponent))
            .collect();

        RowGen {
            event_w: model.events.iter().map(|e| e.weight).collect(),
            url_w: model.urls.iter().map(|e| e.weight).collect(),
            host_w: model.hosts.iter().map(|e| e.weight).collect(),
            lib_w: model.libs.iter().map(|e| e.weight).collect(),
            model,
            persons,
            person_w,
            tenant,
            rng,
        }
    }

    pub fn next(&mut self) -> Row {
        let event = self.model.events[self.rng.weighted(&self.event_w)]
            .value
            .clone();
        let path = self.model.urls[self.rng.weighted(&self.url_w)]
            .value
            .clone();
        let host = self.model.hosts[self.rng.weighted(&self.host_w)]
            .value
            .clone();
        let lib = self.model.libs[self.rng.weighted(&self.lib_w)]
            .value
            .clone();

        let person = self.persons[self.rng.weighted(&self.person_w)];
        let distinct_id = format!("person-{}-{:06}", self.tenant, person);

        let current_url = format!("https://{host}{path}");

        // A deterministic UUID: the RNG is the only entropy in this tool, and a v4
        // from the system would make every run produce a different file.
        let hi = self.rng.next_u64();
        let lo = self.rng.next_u64();
        let uuid = format!(
            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
            (hi >> 32) as u32,
            (hi >> 16) as u16,
            (hi & 0x0fff) as u16,
            ((lo >> 48) as u16 & 0x3fff) | 0x8000,
            lo & 0xffff_ffff_ffff,
        );

        // Valid, compact JSON. The three promoted keys carry exactly the values the
        // `mat_*` columns do — that equality is the fixture's one hard promise about
        // properties, and `tests/generate.rs` asserts it row by row.
        let properties = format!(
            r#"{{"$current_url":"{current_url}","$host":"{host}","$lib":"{lib}","$lib_version":"1.{}.{}","$screen_height":{},"$screen_width":{},"$browser":"{}"}}"#,
            self.rng.below(40),
            self.rng.below(20),
            720 + self.rng.below(9) * 60,
            1280 + self.rng.below(9) * 80,
            ["Chrome", "Firefox", "Safari", "Edge"][self.rng.below(4) as usize],
        );

        let elements_chain = if event == "$autocapture" {
            format!(
                "button:nth-child=\"{}\"nth-of-type=\"{}\"text=\"Submit\";form:nth-child=\"1\"",
                self.rng.below(8) + 1,
                self.rng.below(4) + 1,
            )
        } else {
            String::new()
        };

        Row {
            event,
            distinct_id,
            uuid,
            properties,
            elements_chain,
            current_url,
            host,
            lib,
        }
    }
}
