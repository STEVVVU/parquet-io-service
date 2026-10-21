use bytes::Bytes;
use futures::{stream, StreamExt};
use parpulse_client::RequestParams;
use storage_node::{
    cache::data_store_cache::cache_key_from_request, disk::disk_manager::DiskManager,
};

#[tokio::test]
async fn fixture_requests_cannot_read_project_source() {
    use storage_node::storage_reader::{s3_diskmock::MockS3Reader, AsyncStorageReader};
    let reader = MockS3Reader::new("tests-parquet".into(), vec!["../../Cargo.toml".into()]).await;
    assert!(
        reader.read_all().await.is_err(),
        "fixture request exposed project files"
    );
}

#[test]
fn cache_identity_separates_source_and_object_boundaries() {
    let requests = [
        RequestParams::S3(("a-b".into(), vec!["c".into()])),
        RequestParams::S3(("a".into(), vec!["b-c".into()])),
        RequestParams::S3(("a".into(), vec!["b,c".into()])),
        RequestParams::S3(("a".into(), vec!["b".into(), "c".into()])),
        RequestParams::MockS3(("a-b".into(), vec!["c".into()])),
    ];
    let keys: std::collections::HashSet<_> = requests.iter().map(cache_key_from_request).collect();
    assert_eq!(keys.len(), requests.len());
    assert_eq!(
        cache_key_from_request(&requests[0]),
        cache_key_from_request(&requests[0])
    );
}

#[tokio::test]
async fn failed_stream_never_publishes_partial_cache_and_can_retry() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("object");
    let path = target.to_str().unwrap();
    let manager = DiskManager::default();
    let broken = stream::iter(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(std::io::Error::other("injected source failure").into()),
    ])
    .boxed();
    assert!(manager
        .write_bytes_and_stream_to_disk(None, Some(broken), path)
        .await
        .is_err());
    assert!(
        !target.exists(),
        "failed source exposed a partial final object"
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    manager
        .write_bytes_and_stream_to_disk(Some(vec![Bytes::from_static(b"complete")]), None, path)
        .await
        .unwrap();
    assert_eq!(std::fs::read(target).unwrap(), b"complete");
}

#[tokio::test]
async fn replacement_write_truncates_old_tail() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("object");
    let path = target.to_str().unwrap();
    let manager = DiskManager::default();
    manager
        .write_disk_all(path, b"old long content")
        .await
        .unwrap();
    manager.write_disk_all(path, b"new").await.unwrap();
    assert_eq!(std::fs::read(target).unwrap(), b"new");
}

#[tokio::test]
async fn concurrent_publication_never_overwrites_a_winner() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("object");
    let path = target.to_str().unwrap();
    let manager = DiskManager::default();
    let (a, b) = tokio::join!(
        manager.write_bytes_and_stream_to_disk(
            Some(vec![Bytes::from_static(b"first")]),
            None,
            path
        ),
        manager.write_bytes_and_stream_to_disk(
            Some(vec![Bytes::from_static(b"second")]),
            None,
            path
        )
    );
    assert_ne!(a.is_ok(), b.is_ok(), "exactly one publication must succeed");
    let actual = std::fs::read(target).unwrap();
    assert!(actual == b"first" || actual == b"second");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn dropping_cache_reader_releases_its_pin() {
    use std::{sync::Arc, time::Duration};
    use storage_node::cache::{
        data_store_cache::memdisk::{data_store::disk::DiskStore, MemDiskStoreReplacerValue},
        replacer::{lru::LruReplacer, DataStoreReplacer},
    };
    use tokio::sync::Mutex;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("object");
    std::fs::write(&path, b"cached").unwrap();
    let key = "remote".to_owned();
    let mut replacer = LruReplacer::new(6);
    replacer.put(
        key.clone(),
        MemDiskStoreReplacerValue::new(path.display().to_string(), 6),
    );
    assert!(replacer.pin(&key, 1));
    let replacer = Arc::new(Mutex::new(replacer));
    let store = DiskStore::new(DiskManager::default(), dir.path().display().to_string(), 1);
    let receiver = store
        .read_data(path.to_str().unwrap(), replacer.clone(), key.clone())
        .await
        .unwrap()
        .unwrap();
    drop(receiver);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if replacer
                .lock()
                .await
                .put(
                    "replacement".into(),
                    MemDiskStoreReplacerValue::new("unused".into(), 6),
                )
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled stream leaked its cache pin");
    assert!(replacer.lock().await.peek(&key).is_none());
}
