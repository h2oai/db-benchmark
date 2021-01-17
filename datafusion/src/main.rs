use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::{CsvFile, MemTable};
use datafusion::error::Result;
use datafusion::prelude::*;
use std::env;
use std::time::Instant;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let data = format!("../data/{}.csv", env::var("SRC_DATANAME").unwrap());

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Utf8, false),
        Field::new("id2", DataType::Utf8, false),
        Field::new("id3", DataType::Utf8, false),
        Field::new("id4", DataType::Int32, false),
        Field::new("id5", DataType::Int32, false),
        Field::new("id6", DataType::Int32, false),
        Field::new("v1", DataType::Int32, false),
        Field::new("v2", DataType::Int32, false),
        Field::new("v3", DataType::Float64, false),
    ]);
    let options = CsvReadOptions::new().schema(&schema).has_header(true);

    let csv = CsvFile::try_new(&data, options).unwrap();
    let batch_size = 65536;
    let memtable = MemTable::load(&csv, batch_size).await?;
    ctx.register_table("t", Box::new(memtable));

    // "q1"
    let start = Instant::now();
    let df = ctx.sql("SELECT id1, SUM(v1) AS v1 FROM t GROUP BY id1")?;

    let _results = df.collect().await?;

    println!("q1 took {} ms", start.elapsed().as_millis());

    // "q2"
    let start = Instant::now();
    let df = ctx.sql("SELECT id1, id2, SUM(v1) AS v1 FROM t GROUP BY id1, id2")?;

    let _results = df.collect().await?;

    println!("q2 took {} ms", start.elapsed().as_millis());

    // "q3"
    let start = Instant::now();
    let df = ctx.sql("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM t GROUP BY id3")?;

    let _results = df.collect().await?;

    println!("q3 took {} ms", start.elapsed().as_millis());

    // "q4"
    let start = Instant::now();
    let df =
        ctx.sql("SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM t GROUP BY id4")?;

    let _results = df.collect().await?;

    println!("q4 took {} ms", start.elapsed().as_millis());

    // "q5"
    let start = Instant::now();
    let df =
        ctx.sql("SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM t GROUP BY id6")?;

    let _results = df.collect().await?;

    println!("q5 took {} ms", start.elapsed().as_millis());

    Ok(())
}
