use std::error::Error;
use storage_node::common::config::{ParpulseConfig, ParpulseConfigDataStore};
use storage_node::server::storage_node_serve;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port: u16 = std::env::args().nth(1).ok_or("missing port")?.parse()?;
    let cache_bytes = std::env::args()
        .nth(2)
        .map(|v| v.parse::<usize>())
        .transpose()?
        .unwrap_or(8 * 1024 * 1024);
    if !(8 * 1024 * 1024..=512 * 1024 * 1024).contains(&cache_bytes) {
        return Err("cache bytes must be 8..512 MiB".into());
    }
    let config = ParpulseConfig {
        data_store: ParpulseConfigDataStore::Disk,
        data_store_cache_num: Some(1),
        disk_cache_size: Some(cache_bytes),
        cache_path: Some("owned-cache".into()),
        max_disk_reader_buffer_size: Some(8192),
        ..ParpulseConfig::default()
    };
    storage_node_serve("127.0.0.1", port, config).await?;
    Ok(())
}
