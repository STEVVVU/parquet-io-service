use aws_sdk_s3::{
    config::{retry::RetryConfig, BehaviorVersion, Credentials, Region},
    Client,
};
use futures::StreamExt;
use storage_node::storage_reader::{s3::S3Reader, AsyncStorageReader};
use warp::{
    http::{Method, Response, StatusCode},
    hyper::Body,
    Filter,
};

#[tokio::test]
async fn sdk_head_get_stream_and_failure_use_only_owned_loopback() {
    let object_path = warp::path!("fixture" / String);
    let route = object_path
        .and(warp::method())
        .map(|key: String, method: Method| {
            let data = match key.as_str() {
                "one" => Some("first"),
                "two" => Some("second"),
                _ => None,
            };
            match data {
                Some(data) => Response::builder()
                    .header("content-length", data.len())
                    .body(if method == Method::HEAD {
                        Body::empty()
                    } else {
                        Body::from(data)
                    })
                    .unwrap(),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header("content-type", "application/xml")
                    .body(Body::from(
                        "<Error><Code>NoSuchKey</Code><Message>missing fixture</Message></Error>",
                    ))
                    .unwrap(),
            }
        });
    let (address, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
    let task = tokio::spawn(server);
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            "fixture",
            "fixture",
            None,
            None,
            "owned-test",
        ))
        .endpoint_url(format!("http://{address}"))
        .force_path_style(true)
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build();
    let client = Client::from_conf(config);
    let reader = || {
        S3Reader::with_client(
            client.clone(),
            "fixture".into(),
            vec!["one".into(), "two".into()],
        )
    };
    assert_eq!(reader().get_object_size().await.unwrap(), 11);
    let objects = reader().read_all().await.unwrap();
    assert_eq!(&objects[0][..], b"first");
    assert_eq!(&objects[1][..], b"second");
    let mut stream = reader().into_stream().await.unwrap();
    let mut actual = Vec::new();
    while let Some(chunk) = stream.next().await {
        actual.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(actual, b"firstsecond");
    let mut broken = S3Reader::with_client(client, "fixture".into(), vec!["missing".into()])
        .into_stream()
        .await
        .unwrap();
    assert!(broken.next().await.unwrap().is_err());
    assert!(
        broken.next().await.is_none(),
        "failed SDK stream must terminate"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}
