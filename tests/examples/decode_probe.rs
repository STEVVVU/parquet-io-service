use parpulse_client::client::StorageClientImpl;
use parpulse_client::RequestParams;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = std::env::args().nth(1).ok_or("missing endpoint")?;
    let client = StorageClientImpl::new(&endpoint, &endpoint)?;
    let mut receiver = client
        .request_object_checked(RequestParams::MockS3((
            "tests-parquet".into(),
            vec!["userdata1.parquet".into()],
        )))
        .await?;
    let mut rows = 0;
    let mut batches = 0;
    while let Some(batch) = receiver.recv().await {
        let batch = batch?;
        assert_eq!(batch.num_columns(), 13);
        rows += batch.num_rows();
        batches += 1;
    }
    assert_eq!(rows, 1000, "fixture lost decoded Parquet rows");
    println!("PARQUET_ROWS={rows} COLUMNS=13 BATCHES={batches}");
    Ok(())
}
