use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use istziio_client::client_api::{DataRequest, StorageRequest};
use parpulse_client::client::StorageClientImpl;
use parpulse_client::RequestParams;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    file::properties::WriterProperties,
};
use std::{io::Write, sync::Arc};

fn corrupt_page() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(1)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&bytes).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let offset = reader.metadata().row_group(1).column(0).data_page_offset() as usize;
    bytes[offset..offset + 8].fill(0xff);
    bytes
}

#[tokio::test]
async fn legacy_api_reports_decode_error_instead_of_successful_eof() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock(
            "GET",
            "/file?bucket=tests-parquet&keys=userdata1.parquet&is_test=true",
        )
        .with_body(corrupt_page())
        .create_async()
        .await;
    let client = StorageClientImpl::new(&server.url(), &server.url()).unwrap();
    let response = client
        .request_data_test(StorageRequest::new(0, DataRequest::Table(0)))
        .await;
    assert!(
        response.is_err(),
        "corrupt data was exposed as a successful batch stream"
    );
    mock.assert_async().await;
}

#[tokio::test]
async fn checked_api_reports_late_decoder_errors() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock(
            "GET",
            "/file?bucket=tests-parquet&keys=broken.parquet&is_test=true",
        )
        .with_body(corrupt_page())
        .create_async()
        .await;
    let client = StorageClientImpl::new(&server.url(), &server.url()).unwrap();
    let mut receiver = client
        .request_object_checked(RequestParams::MockS3((
            "tests-parquet".into(),
            vec!["broken.parquet".into()],
        )))
        .await
        .unwrap();
    let mut failed = false;
    while let Some(batch) = receiver.recv().await {
        if batch.is_err() {
            failed = true;
            assert!(receiver.recv().await.is_none());
            break;
        }
    }
    assert!(failed, "decoder error disappeared into EOF");
    mock.assert_async().await;
}

#[tokio::test]
async fn checked_api_rejects_http_errors_and_ambiguous_multiobject_requests() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/file?bucket=bucket&keys=missing")
        .with_status(404)
        .create_async()
        .await;
    let client = StorageClientImpl::new(&server.url(), &server.url()).unwrap();
    assert!(client
        .request_object_checked(RequestParams::S3(("bucket".into(), vec!["missing".into()])))
        .await
        .is_err());
    assert!(client
        .request_object_checked(RequestParams::S3((
            "bucket".into(),
            vec!["a".into(), "b".into()]
        )))
        .await
        .is_err());
    assert!(client
        .request_data_test(StorageRequest::new(0, DataRequest::Table(999)))
        .await
        .is_err());
    mock.assert_async().await;
}
