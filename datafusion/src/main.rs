use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::MemTable,
};
use std::time::Instant;
use std::{env, sync::Arc};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

async fn exec_query(ctx: &mut ExecutionContext, query: &str, name: &str) -> Result<()> {
    let start = Instant::now();

    let ans = ctx.sql(query).await?.collect().await?;

    // TODO: print details

    println!("{} took {} ms", name, start.elapsed().as_millis());

    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let batch_size = 65536;
    let partition_size = num_cpus::get();
    let cfg = ExecutionConfig::new()
        .with_target_partitions(partition_size)
        .with_batch_size(batch_size);
    let mut ctx = ExecutionContext::with_config(cfg);
    let data = format!("../{}.csv", env::var("SRC_DATANAME").unwrap());

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

    let df = ctx.read_csv(&data, options).await?;
    let batches = df.collect_partitioned().await?;
    let memtbl = MemTable::try_new(Arc::new(schema), batches)?;
    ctx.register_table("tbl", Arc::new(memtbl))?;

    exec_query(
        &mut ctx,
        "SELECT id1, SUM(v1) AS v1 FROM tbl GROUP BY id1",
        "q1",
    )
    .await?;
    exec_query(
        &mut ctx,
        "SELECT id1, id2, SUM(v1) AS v1 FROM tbl GROUP BY id1, id2",
        "q2",
    )
    .await?;
    exec_query(
        &mut ctx,
        "SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM tbl GROUP BY id3",
        "q3",
    )
    .await?;
    exec_query(
        &mut ctx,
        "SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM tbl GROUP BY id4",
        "q4",
    )
    .await?;
    exec_query(
        &mut ctx,
        "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM tbl GROUP BY id6",
        "q5",
    )
    .await?;
    exec_query(
        &mut ctx,
        "SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM tbl GROUP BY id3",
        "q7",
    )
    .await?;

    exec_query(&mut ctx, "SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(*) AS cnt FROM tbl GROUP BY id1, id2, id3, id4, id5, id6", "q10").await?;

    Ok(())
}
