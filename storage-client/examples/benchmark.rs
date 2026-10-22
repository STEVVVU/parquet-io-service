use anyhow::{ensure, Result};
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;

fn value(row: usize, column: usize) -> i64 {
    ((row as u64 * 7919 + column as u64 * 104729) % 1_000_003) as i64
}

fn generate(path: &str, rows: usize, row_group: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(
        (0..16)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let properties = WriterProperties::builder()
        .set_max_row_group_size(row_group)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(properties))?;
    for start in (0..rows).step_by(8192) {
        let end = (start + 8192).min(rows);
        let columns = (0..16)
            .map(|c| {
                Arc::new(Int64Array::from(
                    (start..end).map(|r| value(r, c)).collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect();
        writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    writer.close()?;
    Ok(())
}

fn decode(path: &str, columns: usize, first_group: bool) -> Result<(usize, i64)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let projection = ProjectionMask::leaves(builder.parquet_schema(), 0..columns);
    let mut builder = builder.with_projection(projection).with_batch_size(8192);
    if first_group {
        builder = builder.with_row_groups(vec![0]);
    }
    let mut rows = 0;
    let mut sum = 0;
    for batch in builder.build()? {
        let batch = batch?;
        for (column_index, column) in batch.columns().iter().enumerate() {
            let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
            for (offset, actual) in column.values().iter().enumerate() {
                ensure!(
                    *actual == value(rows + offset, column_index),
                    "decoded value or row assignment differs"
                );
            }
            sum += column.values().iter().sum::<i64>();
        }
        rows += batch.num_rows();
    }
    Ok((rows, sum))
}

async fn read(
    client: reqwest::Client,
    source: String,
    columns: usize,
    first_group: bool,
) -> Result<(u64, usize, i64, u128, u128)> {
    let start = Instant::now();
    if let Some(path) = source.strip_prefix("file://") {
        let path = path.to_owned();
        let (rows, sum) =
            tokio::task::spawn_blocking(move || decode(&path, columns, first_group)).await??;
        return Ok((0, rows, sum, 0, start.elapsed().as_micros()));
    }
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("download.parquet");
    let mut file = tokio::fs::File::create(&path).await?;
    let mut response = client
        .get(source)
        .send()
        .await?
        .error_for_status()?
        .bytes_stream();
    let mut bytes = 0;
    while let Some(chunk) = response.next().await {
        let chunk = chunk?;
        bytes += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    drop(file);
    let downloaded = start.elapsed().as_micros();
    let decode_start = Instant::now();
    let (rows, sum) = tokio::task::spawn_blocking(move || {
        let result = decode(path.to_str().unwrap(), columns, first_group);
        drop(temp);
        result
    })
    .await??;
    Ok((
        bytes,
        rows,
        sum,
        downloaded,
        decode_start.elapsed().as_micros(),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    ensure!(args.len() >= 5, "generate PATH ROWS ROWGROUP | read SOURCE ROWS ROWGROUP COLUMNS GROUP CONCURRENCY RATE DURATION CSV");
    let rows = args[3].parse::<usize>()?;
    let row_group = args[4].parse::<usize>()?;
    ensure!(
        (1..=1_000_000).contains(&rows) && (1..=65536).contains(&row_group),
        "bounded rows and row groups required"
    );
    if args[1] == "generate" {
        return generate(&args[2], rows, row_group);
    }
    ensure!(
        args[1] == "read" && args.len() == 12,
        "invalid read arguments"
    );
    let source = &args[2];
    let columns = args[5].parse::<usize>()?;
    let first_group = args[6] == "first";
    ensure!(columns == 1 || columns == 16, "columns must be 1 or 16");
    ensure!(
        first_group || args[6] == "all",
        "group must be first or all"
    );
    let concurrency = args[7].parse::<usize>()?;
    let rate = args[8].parse::<usize>()?;
    let duration = args[9].parse::<f64>()?;
    let requests = (duration * rate as f64) as usize;
    ensure!(
        (1..=32).contains(&concurrency)
            && (1..=100000).contains(&rate)
            && duration.is_finite()
            && duration > 0.0
            && duration <= 120.0
            && (1..=100000).contains(&requests),
        "invalid bounded load configuration"
    );
    ensure!(args[10] == "--out", "expected --out CSV");
    let expected_rows = if first_group {
        rows.min(row_group)
    } else {
        rows
    };
    let expected_sum = (0..expected_rows)
        .flat_map(|r| (0..columns).map(move |c| value(r, c)))
        .sum::<i64>();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = tokio::task::JoinSet::new();
    let mut samples = Vec::new();
    let start = Instant::now();
    for index in 0..requests {
        let scheduled = std::time::Duration::from_secs_f64(index as f64 / rate as f64);
        tokio::time::sleep_until((start + scheduled).into()).await;
        let actual_start = start.elapsed().as_micros();
        if let Ok(permit) = permits.clone().try_acquire_owned() {
            let client = client.clone();
            let source = source.clone();
            tasks.spawn(async move {
                let result = read(client, source, columns, first_group).await;
                drop(permit);
                (
                    index,
                    scheduled.as_micros(),
                    actual_start,
                    start.elapsed().as_micros(),
                    result,
                )
            });
        } else {
            samples.push(format!(
                "{index},{},{actual_start},0,0,0,0,0,0,rejected",
                scheduled.as_micros()
            ));
        }
    }
    let mut failed = 0;
    let mut successful = 0;
    while let Some(task) = tasks.join_next().await {
        let (index, scheduled, actual_start, finished, result) = task?;
        match result {
            Ok((bytes, actual_rows, sum, download_us, decode_us)) => {
                ensure!(
                    actual_rows == expected_rows && sum == expected_sum,
                    "incorrect decoded result"
                );
                successful += 1;
                samples.push(format!(
                    "{index},{scheduled},{actual_start},{},{bytes},{actual_rows},{sum},{download_us},{decode_us},ok",
                    finished - scheduled
                ));
            }
            Err(error) => {
                failed += 1;
                eprintln!("request {index}: {error:#}");
                samples.push(format!(
                    "{index},{scheduled},{actual_start},{},0,0,0,0,0,error",
                    finished - scheduled
                ));
            }
        }
    }
    tokio::time::sleep_until((start + std::time::Duration::from_secs_f64(duration)).into()).await;
    let wall_us = start.elapsed().as_micros();
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[11])?;
    writeln!(
        output,
        "index,scheduled_us,started_us,elapsed_us,http_bytes,rows,sum,download_us,decode_us,status"
    )?;
    for sample in samples {
        writeln!(output, "{sample}")?;
    }
    println!(
        "offered={requests} successful={successful} failed={failed} wall_us={}",
        wall_us
    );
    ensure!(failed == 0, "HTTP or decode errors occurred");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_row_groups_and_swapped_row_control() -> Result<()> {
        let work = tempfile::tempdir()?;
        let path = work.path().join("fixture.parquet");
        generate(path.to_str().unwrap(), 19, 4)?;
        assert_eq!(decode(path.to_str().unwrap(), 1, true)?.0, 4);
        assert_eq!(decode(path.to_str().unwrap(), 16, false)?.0, 19);
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int64, false)]));
        let mut writer = ArrowWriter::try_new(File::create(&path)?, schema.clone(), None)?;
        writer.write(&RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![value(1, 0), value(0, 0)]))],
        )?)?;
        writer.close()?;
        assert!(
            decode(path.to_str().unwrap(), 1, false).is_err(),
            "row swaps with unchanged sum must fail"
        );
        Ok(())
    }
}
