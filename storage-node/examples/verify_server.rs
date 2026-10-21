use std::error::Error;
use storage_node::common::config::{ParpulseConfig, ParpulseConfigDataStore};
use storage_node::server::storage_node_serve;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port: u16 = std::env::args().nth(1).ok_or("missing port")?.parse()?;
    let config = ParpulseConfig {
        data_store: ParpulseConfigDataStore::Disk,
        data_store_cache_num: Some(1),
        disk_cache_size: Some(8 * 1024 * 1024),
        cache_path: Some("owned-cache".into()),
        max_disk_reader_buffer_size: Some(8192),
        ..ParpulseConfig::default()
    };
    storage_node_serve("127.0.0.1", port, config).await?;
    Ok(())
}
