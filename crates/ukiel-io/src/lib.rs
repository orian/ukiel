//! Parquet range I/O shared by rewrite and MV workers. Kept outside ukiel-core
//! so offline schema/writer users do not inherit an object-store client.
use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, future::BoxFuture};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataSuffixFetch},
    },
    errors::{ParquetError, Result},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

/// Replacement for Parquet's deprecated object-store reader, using the range
/// and footer APIs documented by AsyncFileReader. No detached I/O or local
/// cache is introduced. External errors retain their concrete object-store
/// cause, including NotFound, for a caller's recovery decisions.
#[derive(Debug, Clone)]
pub struct ParquetObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ParquetObjectReader {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

fn external(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for ParquetObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(external)
            .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(external)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await
                .map(Arc::new)
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ParquetObjectReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            let options = GetOptions {
                range: Some(GetRange::Suffix(suffix as u64)),
                ..Default::default()
            };
            self.store
                .get_opts(&self.path, options)
                .await
                .map_err(external)?
                .bytes()
                .await
                .map_err(external)
        }
        .boxed()
    }
}
