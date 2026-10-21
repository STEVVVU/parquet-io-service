use async_trait::async_trait;
use bytes::Bytes;
use parpulse_client::RequestParams;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::Receiver;

use crate::error::ParpulseResult;

pub mod memdisk;
pub mod sqlite;

#[async_trait]
pub trait DataStoreCache {
    async fn get_data_from_cache(
        &self,
        request_param: &RequestParams,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>>;

    /// Put data to cache. Accepts a stream of bytes and returns the number of bytes written.
    /// The data_size parameter is optional and can be used to hint the cache about the size of the data.
    /// If the data_size is not provided, the cache implementation should try to determine the size of
    /// the data.
    async fn put_data_to_cache(&self, request_param: &RequestParams) -> ParpulseResult<usize>;
}

pub fn cache_key_from_request(request_param: &RequestParams) -> String {
    let (kind, bucket, keys) = match request_param {
        RequestParams::S3((bucket, keys)) => (0u8, bucket, keys),
        RequestParams::MockS3((bucket, keys)) => (1u8, bucket, keys),
    };
    let mut hash = Sha256::new();
    hash.update([kind]);
    hash.update((keys.len() as u64).to_be_bytes());
    for part in std::iter::once(bucket).chain(keys.iter()) {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
