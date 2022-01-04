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

    // let ans = ctx.sql(query).await?.collect().await?;
    let ans = ctx.sql(query).await?.show().await?;

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
    let files = env::var("SRC_DATANAME")
        .unwrap()
        .split(",")
        .map(|file| format!("../data/{}.csv", file))
        .collect::<Vec<String>>();

    // Left
    let left_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("id3", DataType::Int32, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("id6", DataType::Utf8, false),
        Field::new("v1", DataType::Float32, false),
    ]);
    let options = CsvReadOptions::new().schema(&left_schema).has_header(true);
    let df = ctx.read_csv(&files[3], options).await?;
    let batches = df.collect_partitioned().await?;
    let left = MemTable::try_new(Arc::new(left_schema), batches)?;
    ctx.register_table("left", Arc::new(left))?;
    println!("Registered left table: {:?}", &files[3]);

    // Small
    let small_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("v2", DataType::Float32, false),
    ]);
    let options = CsvReadOptions::new().schema(&small_schema).has_header(true);
    let df = ctx.read_csv(&files[0], options).await?;
    let batches = df.collect_partitioned().await?;
    let small = MemTable::try_new(Arc::new(small_schema), batches)?;
    ctx.register_table("small", Arc::new(small))?;
    println!("Registered small table: {:?}", &files[0]);

    // Medium
    let medium_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("v2", DataType::Float32, false),
    ]);
    let options = CsvReadOptions::new()
        .schema(&medium_schema)
        .has_header(true);
    let df = ctx.read_csv(&files[1], options).await?;
    let batches = df.collect_partitioned().await?;
    let medium = MemTable::try_new(Arc::new(medium_schema), batches)?;
    ctx.register_table("medium", Arc::new(medium))?;
    println!("Registered medium table: {:?}", &files[1]);

    //Large
    let large_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("id3", DataType::Int32, false),
        Field::new("id4", DataType::Utf8, false),
        Field::new("id5", DataType::Utf8, false),
        Field::new("id6", DataType::Utf8, false),
        Field::new("v2", DataType::Float32, false),
    ]);
    let options = CsvReadOptions::new().schema(&large_schema).has_header(true);
    let df = ctx.read_csv(&files[2], options).await?;
    let batches = df.collect_partitioned().await?;
    let large = MemTable::try_new(Arc::new(large_schema), batches)?;
    ctx.register_table("large", Arc::new(large))?;
    println!("Registered large table: {:?}", &files[2]);

    exec_query(
        &mut ctx,
        "SELECT left.id1, left.id2, left.id3, left.id4, small.id4, left.id5, left.id6, left.v1, small.v2 FROM left INNER JOIN small ON left.id1 = small.id1;",
        "q1",
    )
    .await?;

    // exec_query(
    //     &mut ctx,
    //     "SELECT left.id1, medium.id1, left.id2, left.id3, left.id4, medium.id4, left.id5, medium.id5, left.id6, left.v1, medium.v2 FROM left INNER JOIN medium ON left.id2 = medium.id2;",
    //     "q2",
    // )
    // .await?;

    // exec_query(
    //     &mut ctx,
    //     "SELECT left.id1, medium.id1, left.id2, left.id3, left.id4, medium.id4, left.id5, medium.id5, left.id6, left.v1, medium.v2 FROM left INNER JOIN medium ON left.id2 = medium.id2;",
    //     "q3",
    // )
    // .await?;

    // exec_query(
    //     &mut ctx,
    //     "SELECT left.id1, medium.id1, left.id2, left.id3, left.id4, medium.id4, left.id5, medium.id5, left.id6, left.v1, medium.v2 FROM left LEFT JOIN medium ON left.id5 = medium.id5;",
    //     "q4",
    // )
    // .await?;

    // exec_query(
    //     &mut ctx,
    //     "SELECT left.id1, large.id1, left.id2, large.id2, left.id3, left.id4, large.id4, left.id5, large.id5, left.id6, large.id6, left.v1, large.v2 FROM left LEFT JOIN large ON left.id3 = large.id3;",
    //     "q5",
    // )
    // .await?;

    Ok(())
}
