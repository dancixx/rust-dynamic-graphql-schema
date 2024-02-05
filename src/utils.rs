use std::time::SystemTime;

use async_graphql::dynamic::TypeRef;
use chrono::DateTime;
use tokio_postgres::Row;

pub fn col_type(pg_type: &str) -> TypeRef {
    match pg_type {
        "bool" => TypeRef::named(TypeRef::BOOLEAN),
        "varchar" | "char(n)" | "text" | "name" => TypeRef::named(TypeRef::STRING),
        "char" => TypeRef::named(TypeRef::STRING),
        "int2" | "smallserial" | "smallint" => TypeRef::named(TypeRef::INT),
        "integer" | "int" | "int4" | "serial" => TypeRef::named(TypeRef::INT),
        "int8" | "bigserial" | "bigint" => TypeRef::named(TypeRef::INT),
        "float4" | "real" => TypeRef::named(TypeRef::FLOAT),
        "float8" | "double precision" => TypeRef::named(TypeRef::FLOAT),
        "timestamp" | "timestamptz" => TypeRef::named(TypeRef::STRING),
        _ => TypeRef::named(TypeRef::STRING),
    }
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
        "integer" | "int" | "int4" | "serial" => {
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
    value.unwrap_or("NULL".to_string())
}
