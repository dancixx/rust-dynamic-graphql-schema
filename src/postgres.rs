use std::{
    collections::{BTreeMap, HashMap},
    env,
    time::SystemTime,
};

use anyhow::Result;
use chrono::DateTime;
use tokio_postgres::{Client, NoTls, Row};

use crate::error::{ConnectionError, DriverError, EnvironmentError};

pub async fn connector<'a>() -> Result<Client, DriverError<'a>> {
    let c_string = format!(
        "host={} user={} password={} port={}",
        env::var("DB_HOST").map_err(|_| DriverError::EnvironmentError(
            EnvironmentError::MissingEnvironmentVariable("Missing environment variable DB_HOST")
        ))?,
        env::var("DB_USER").map_err(|_| DriverError::EnvironmentError(
            EnvironmentError::MissingEnvironmentVariable("Missing environment variable DB_USER")
        ))?,
        env::var("DB_PASSWORD").map_err(|_| DriverError::EnvironmentError(
            EnvironmentError::MissingEnvironmentVariable(
                "Missing environment variable DB_PASSWORD"
            )
        ))?,
        env::var("DB_PORT").map_err(|_| DriverError::EnvironmentError(
            EnvironmentError::MissingEnvironmentVariable("Missing environment variable DB_PORT")
        ))?,
    );
    let (client, connection) = tokio_postgres::connect(&c_string, NoTls)
        .await
        .map_err(|e| DriverError::ConnectionError(ConnectionError::PostgresError(e)))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn get_tables(client: &Client) -> Result<BTreeMap<String, Vec<String>>> {
    let query = client
        .query(
            r#"
                SELECT table_name, string_agg(column_name, ', ') AS columns
                FROM information_schema.columns
                WHERE table_schema = 'noexapp'
                GROUP BY table_name
                ORDER BY table_name;
            "#,
            &[],
        )
        .await?;

    let mut tables = BTreeMap::new();
    for row in query.iter() {
        let table = row.get::<_, String>(0);
        let cols = row.get::<_, String>(1);
        let cols = cols.split(", ").map(String::from).collect::<Vec<String>>();
        tables.insert(table, cols);
    }

    Ok(tables)
}

pub async fn get_relations(client: &Client) -> Result<HashMap<String, Vec<(String, String)>>> {
    let relations = client
        .query(
            r#"
                SELECT tc.constraint_name, tc.table_name, kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                WHERE constraint_type = 'FOREIGN KEY';
            "#,
            &[],
        )
        .await?;

    let relation_by_tables = relations
        .iter()
        .map(|row| {
            let _constraint_name = row.get::<_, String>(0);
            let table = row.get::<_, String>(1);
            let col_name = row.get::<_, String>(2);
            let foreign_table = row.get::<_, String>(3);
            let foreign_col_name = row.get::<_, String>(4);

            ((table, col_name), (foreign_table, foreign_col_name))
        })
        .collect::<Vec<_>>();

    // group by table name
    let mut relations = HashMap::<String, Vec<(String, String)>>::new();
    for (table, foreign_table) in relation_by_tables {
        let mut found = false;
        for (table_name, foreign_tables) in relations.iter_mut() {
            if table_name == &table.0 {
                found = true;
                foreign_tables.push((
                    format!("{}_{}", &table.0, &table.1),
                    format!("{}", &foreign_table.0),
                ));
            }
        }
        if !found {
            relations.insert(
                format!("{}", &table.0),
                vec![(
                    format!("{}_{}", &table.0, &table.1),
                    format!("{}", &foreign_table.0),
                )],
            );
        }
    }
    println!("{:?}", relations);
    Ok(relations)
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
