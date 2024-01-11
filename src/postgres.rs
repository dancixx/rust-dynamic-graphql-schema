use std::{env, time::SystemTime};

use anyhow::Result;
use chrono::DateTime;
use tokio_postgres::{Client, NoTls, Row};

pub async fn connector() -> Result<Client> {
    let c_string = format!(
        "host={} user={} password={} port={}",
        env::var("DB_HOST")?,
        env::var("DB_USER")?,
        env::var("DB_PASSWORD")?,
        env::var("DB_PORT")?
    );
    let (client, connection) = tokio_postgres::connect(&c_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

pub fn reflective_get(row: &Row, index: usize) -> String {
    let column_type = row.columns().get(index).map(|c| c.type_().name()).unwrap();
    // see https://docs.rs/sqlx/0.4.0-beta.1/sqlx/postgres/types/index.html

    let value = match column_type {
        "bool" => {
            let v = row.get::<_, Option<bool>>(index);
            v.map(|v| v.to_string())
        }
        "varchar" | "char(n)" | "text" | "name" => row.get(index),
        "char" => {
            let v = row.get::<_, i8>(index);
            Some(v.to_string())
        }
        "int2" | "smallserial" | "smallint" => {
            let v = row.get::<_, Option<i16>>(index);
            v.map(|v| v.to_string())
        }
        "int" | "int4" | "serial" => {
            let v = row.get::<_, Option<i32>>(index);
            v.map(|v| v.to_string())
        }
        "int8" | "bigserial" | "bigint" => {
            let v = row.get::<_, Option<i64>>(index);
            v.map(|v| v.to_string())
        }
        "float4" | "real" => {
            let v = row.get::<_, Option<f32>>(index);
            v.map(|v| v.to_string())
        }
        "float8" | "double precision" => {
            let v = row.get::<_, Option<f64>>(index);
            v.map(|v| v.to_string())
        }
        "timestamp" | "timestamptz" => {
            // with-chrono feature is needed for this
            let v = row.get::<_, Option<SystemTime>>(index);
            let v = DateTime::<chrono::Utc>::from(v.unwrap());
            Some(v.to_string())
        }
        &_ => Some("CANNOT PARSE".to_string()),
    };
    value.unwrap_or("null".to_string())
}
