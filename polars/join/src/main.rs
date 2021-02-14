use polars::prelude::*;
use polars::toggle_string_cache;
use std::time::Instant;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

macro_rules! run_query {
    ($name: expr, $query: block) => {{
        let t = Instant::now();
        let (ans, chk) = $query;
        println!("{} took {} ms", $name, t.elapsed().as_millis());
        println!("ans = {:?}\n chk = {:?}", ans, chk);
        (ans, chk)
    }};
}

fn main() -> Result<()> {
    toggle_string_cache(true);

    // join tables should be passed as arguments
    let args: Vec<String> = std::env::args()
        .map(|s| format!("../data/{}.csv", s))
        .collect();
    dbg!(&args);

    let path = format!("../data/{}.csv", std::env::var("SRC_DATANAME").unwrap());

    let overwrite_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32),
        Field::new("id2", DataType::Int32),
        Field::new("id3", DataType::Int32),
        Field::new("v1", DataType::Float64),
    ]);
    let mut x = CsvReader::from_path(&path)?
        .with_dtype_overwrite(Some(&overwrite_schema))
        .finish()?;

    x.may_apply("id4", |s| s.cast::<CategoricalType>())?;
    x.may_apply("id5", |s| s.cast::<CategoricalType>())?;
    x.may_apply("id6", |s| s.cast::<CategoricalType>())?;

    let overwrite_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32),
        Field::new("v2", DataType::Float64),
    ]);
    let mut small = CsvReader::from_path(&args[1])?
        .with_dtype_overwrite(Some(&overwrite_schema))
        .finish()?;
    small.may_apply("id4", |s| s.cast::<CategoricalType>())?;

    let overwrite_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32),
        Field::new("id2", DataType::Int32),
        Field::new("v2", DataType::Float64),
    ]);
    let mut medium = CsvReader::from_path(&args[1])?
        .with_dtype_overwrite(Some(&overwrite_schema))
        .finish()?;
    medium.may_apply("id4", |s| s.cast::<CategoricalType>())?;
    medium.may_apply("id5", |s| s.cast::<CategoricalType>())?;

    let overwrite_schema = Schema::new(vec![
        Field::new("id1", DataType::Int32),
        Field::new("id2", DataType::Int32),
        Field::new("id3", DataType::Int32),
        Field::new("v2", DataType::Float64),
    ]);
    let mut big = CsvReader::from_path(&args[1])?
        .with_dtype_overwrite(Some(&overwrite_schema))
        .finish()?;
    big.may_apply("id4", |s| s.cast::<CategoricalType>())?;
    big.may_apply("id5", |s| s.cast::<CategoricalType>())?;
    big.may_apply("id6", |s| s.cast::<CategoricalType>())?;

    // clear string cache from memory
    toggle_string_cache(false);

    dbg!(
        x.height(),
        small.height(),
        medium.height(),
        big.height(),
        "joining..."
    );

    for _ in 0..2 {
        let _ = run_query!("q1", {
            let ans = x.inner_join(&small, "id1", "id1")?;

            let checks = ans
                .clone()
                .lazy()
                .select(vec![col("v1").sum(), col("v2").sum()])
                .collect()?;
            let sum_v1 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();
            let sum_v2 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();

            let chk = (sum_v1, sum_v2);
            (ans, chk)
        });
    }

    for _ in 0..2 {
        let _ = run_query!("q2", {
            let ans = x.inner_join(&medium, "id2", "id2")?;

            let checks = ans
                .clone()
                .lazy()
                .select(vec![col("v1").sum(), col("v2").sum()])
                .collect()?;
            let sum_v1 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();
            let sum_v2 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();

            let chk = (sum_v1, sum_v2);
            (ans, chk)
        });
    }

    for _ in 0..2 {
        let _ = run_query!("q3", {
            let ans = x.left_join(&medium, "id2", "id2")?;

            let checks = ans
                .clone()
                .lazy()
                .select(vec![col("v1").sum(), col("v2").sum()])
                .collect()?;
            let sum_v1 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();
            let sum_v2 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();

            let chk = (sum_v1, sum_v2);
            (ans, chk)
        });
    }

    for _ in 0..2 {
        let _ = run_query!("q4", {
            let ans = x.left_join(&medium, "id5", "id5")?;

            let checks = ans
                .clone()
                .lazy()
                .select(vec![col("v1").sum(), col("v2").sum()])
                .collect()?;
            let sum_v1 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();
            let sum_v2 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();

            let chk = (sum_v1, sum_v2);
            (ans, chk)
        });
    }

    for _ in 0..2 {
        let _ = run_query!("q6", {
            let ans = x.inner_join(&medium, "id3", "id3")?;

            let checks = ans
                .clone()
                .lazy()
                .select(vec![col("v1").sum(), col("v2").sum()])
                .collect()?;
            let sum_v1 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();
            let sum_v2 = checks
                .select_at_idx(0)
                .unwrap()
                .cast::<Float32Type>()?
                .f32()?
                .get(0)
                .unwrap();

            let chk = (sum_v1, sum_v2);
            (ans, chk)
        });
    }

    Ok(())
}
