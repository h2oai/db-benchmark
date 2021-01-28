use polars::lazy::functions::pearson_corr;
use polars::prelude::*;
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
    let path =
        std::env::var("CSV_SRC").expect("env var CSV_SRC pointing to the csv_file is not set");

    let overwrite_schema = Schema::new(vec![
        Field::new("id4", DataType::Int32),
        Field::new("id5", DataType::Int32),
        Field::new("id6", DataType::Int32),
        Field::new("v1", DataType::Int32),
        Field::new("v2", DataType::Int32),
        Field::new("v3", DataType::Float64),
    ]);
    let mut df = CsvReader::from_path(&path)?
        .with_dtype_overwrite(Some(&overwrite_schema))
        .finish()?;
    df.may_apply("id1", |s| s.cast::<CategoricalType>())?;
    df.may_apply("id2", |s| s.cast::<CategoricalType>())?;
    df.may_apply("id3", |s| s.cast::<CategoricalType>())?;

    let q1 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id1")])
            .agg(vec![col("v1").sum().alias("v1")])
            .collect()
    };
    let _ = run_query!("q1", {
        let ans = q1()?;
        let chk = (ans.column("v1")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });
    let _ = run_query!("q1", {
        let ans = q1()?;
        let chk = (ans.column("v1")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let q2 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id1"), col("id2")])
            .agg(vec![col("v1").sum().alias("v1")])
            .collect()
    };

    let _ = run_query!("q2", {
        let ans = q2()?;
        let chk = (ans.column("v1")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });
    let _ = run_query!("q2", {
        let ans = q2()?;
        let chk = (ans.column("v1")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let q3 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id3")])
            .agg(vec![
                col("v1").sum().alias("v1_sum"),
                col("v3").mean().alias("v3_mean"),
            ])
            .collect()
    };

    let _ = run_query!("q3", {
        let ans = q3()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![col("v1_sum").sum(), col("v3_mean").sum()])
            .collect()?;
        let sum_v1 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_mean_v3 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_v1, sum_mean_v3);
        (ans, chk)
    });

    let _ = run_query!("q3", {
        let ans = q3()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![col("v1_sum").sum(), col("v3_mean").sum()])
            .collect()?;
        let sum_v1 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_mean_v3 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_v1, sum_mean_v3);
        (ans, chk)
    });

    let q4 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id4")])
            .agg(vec![
                col("v1").mean().alias("v1_mean"),
                col("v2").mean().alias("v2_mean"),
                col("v3").mean().alias("v3_mean"),
            ])
            .collect()
    };

    let _ = run_query!("q4", {
        let ans = q4()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![
                col("v1_mean").sum(),
                col("v2_mean").sum(),
                col("v3_mean").sum(),
            ])
            .collect()?;
        let sum_mean_v1 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_mean_v2 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_mean_v3 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_mean_v1, sum_mean_v2, sum_mean_v3);
        (ans, chk)
    });

    let _ = run_query!("q4", {
        let ans = q4()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![
                col("v1_mean").sum(),
                col("v2_mean").sum(),
                col("v3_mean").sum(),
            ])
            .collect()?;
        let sum_mean_v1 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();

        let sum_mean_v2 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_mean_v3 = checks
            .select_at_idx(2)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_mean_v1, sum_mean_v2, sum_mean_v3);
        (ans, chk)
    });

    let q5 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id6")])
            .agg(vec![
                col("v1").sum().alias("v1_sum"),
                col("v2").sum().alias("v2_sum"),
                col("v3").sum().alias("v3_sum"),
            ])
            .collect()
    };

    let _ = run_query!("q5", {
        let ans = q5()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![
                col("v1_sum").sum(),
                col("v2_sum").sum(),
                col("v3_sum").sum(),
            ])
            .collect()?;
        let sum_sum_v1 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_sum_v2 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_sum_v3 = checks
            .select_at_idx(2)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_sum_v1, sum_sum_v2, sum_sum_v3);
        (ans, chk)
    });

    let _ = run_query!("q5", {
        let ans = q5()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![
                col("v1_sum").sum(),
                col("v2_sum").sum(),
                col("v3_sum").sum(),
            ])
            .collect()?;
        let sum_sum_v1 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_sum_v2 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_sum_v3 = checks
            .select_at_idx(2)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_sum_v1, sum_sum_v2, sum_sum_v3);
        (ans, chk)
    });

    let q6 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id4"), col("id5")])
            .agg(vec![
                col("v3").median().alias("v3_median"),
                col("v3").std().alias("v3_std"),
            ])
            .collect()
    };

    let _ = run_query!("q6", {
        let ans = q6()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![col("v3_median").sum(), col("v3_std").sum()])
            .collect()?;
        let sum_v3_median = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_v3_std = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_v3_median, sum_v3_std);
        (ans, chk)
    });

    let _ = run_query!("q6", {
        let ans = q6()?;
        let checks = ans
            .clone()
            .lazy()
            .select(vec![col("v3_median").sum(), col("v3_std").sum()])
            .collect()?;
        let sum_v3_median = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_v3_std = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_v3_median, sum_v3_std);
        (ans, chk)
    });

    let q7 = || {
        df.clone()
            .lazy()
            .groupby(vec![col("id3")])
            .agg(vec![
                col("v1").max().alias("v1"),
                col("v2").min().alias("v2"),
            ])
            .select(vec![
                col("id3"),
                (col("v1") - col("v2")).alias("range_v1_v2"),
            ])
            .collect()
    };

    let _ = run_query!("q7", {
        let ans = q7()?;

        let chk = (ans.column("range_v1_v2")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let _ = run_query!("q7", {
        let ans = q7()?;

        let chk = (ans.column("range_v1_v2")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let q8 = || {
        df.clone()
            .lazy()
            .drop_nulls(Some(vec![col("v3")]))
            .sort("v3", true)
            .groupby(vec![col("id6")])
            .agg(vec![col("v3").head(Some(2)).alias("v3_top_2")])
            .explode(&[col("v3_top_2")])
            .collect()
    };

    let _ = run_query!("q8", {
        let ans = q8()?;

        let chk = (ans.column("v3_top_2")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let _ = run_query!("q8", {
        let ans = q8()?;

        let chk = (ans.column("v3_top_2")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let q9 = || {
        df.clone()
            .lazy()
            .drop_nulls(Some(vec![col("v1"), col("v2")]))
            .groupby(vec![col("id2"), col("id4")])
            .agg(vec![pearson_corr(col("v1"), col("v2"))
                .pow(2.0)
                .alias("r2")])
            .collect()
    };
    let _ = run_query!("q9", {
        let ans = q9()?;

        let chk = (ans.column("r2")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let _ = run_query!("q9", {
        let ans = q9()?;

        let chk = (ans.column("r2")?.sum::<i32>().unwrap(),);
        (ans, chk)
    });

    let q10 = || {
        df.clone()
            .lazy()
            .groupby(vec![
                col("id1"),
                col("id2"),
                col("id3"),
                col("id4"),
                col("id5"),
                col("id6"),
            ])
            .agg(vec![
                col("v3").sum().alias("v3"),
                col("v1").count().alias("v1"),
            ])
            .collect()
    };
    let _ = run_query!("q10", {
        let ans = q10()?;

        let checks = ans
            .clone()
            .lazy()
            .select(vec![col("v3").sum(), col("v1").sum()])
            .collect()?;
        let sum_v3 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_v1 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_v3, sum_v1);

        (ans, chk)
    });

    let _ = run_query!("q10", {
        let ans = q10()?;

        let checks = ans
            .clone()
            .lazy()
            .select(vec![col("v3").sum(), col("v1").sum()])
            .collect()?;
        let sum_v3 = checks
            .select_at_idx(0)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let sum_v1 = checks
            .select_at_idx(1)
            .unwrap()
            .cast::<Float32Type>()?
            .f32()?
            .get(0)
            .unwrap();
        let chk = (sum_v3, sum_v1);

        (ans, chk)
    });

    Ok(())
}
