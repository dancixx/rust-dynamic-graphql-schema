use async_graphql::{
    dynamic::{self, Field, FieldFuture, FieldValue, Object, TypeRef},
    http::GraphiQLSource,
    Object, Value,
};
use async_graphql_axum::GraphQL;
use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use chrono::DateTime;
use std::{collections::HashMap, error::Error, time::SystemTime};
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, Row};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn new(&self) -> &str {
        "hello world"
    }
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/").finish())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize the logger
    tracing_subscriber::fmt::init();

    let (client, connection) =
        tokio_postgres::connect("host={} user={} password={} port={}", NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let tables = client
        .query(
            r#"
                SELECT table_name, string_agg(column_name, ', ') AS columns
                FROM information_schema.columns
                WHERE table_schema = 'public'
                GROUP BY table_name
                ORDER BY table_name;
            "#,
            &[],
        )
        .await?;

    let tables = tables
        .iter()
        .map(|row| {
            let table_name: &str = row.get(0);
            let columns: &str = row.get(1);
            (table_name, columns.split(", ").collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();

    // generate graphql schema from tables

    let tables = tables
        .iter()
        .map(|(table_name, columns)| {
            let columns = columns
                .iter()
                .map(|column| {
                    let column = column.to_string();
                    let column = column.replace(" ", "_");
                    column
                })
                .collect::<Vec<_>>();
            (table_name.to_string(), columns)
        })
        .collect::<Vec<_>>();
    let mut query = Object::new("Query");
    let mut qs = Vec::new();

    for (table_name, columns) in tables {
        let mut q = Object::new(&table_name);

        let table_name_clone = table_name.clone();
        query = query.field(Field::new(
            table_name.clone(),
            TypeRef::named_list_nn(q.type_name()),
            move |ctx| {
                let table_name = table_name_clone.clone();
                FieldFuture::new(async move {
                    let client = ctx.data::<tokio_postgres::Client>()?;
                    let field = ctx.field().selection_set();
                    let fields = field.into_iter().map(|f| f.name()).collect::<Vec<_>>();
                    let fields_to_string = fields.join(", ");

                    let query_params = format!(
                        "SELECT {} FROM public.{} LIMIT 100;",
                        fields_to_string, table_name
                    );
                    let query = client.query(&query_params, &[]).await?;
                    let rows = query
                        .iter()
                        .map(|row| {
                            let mut vals = HashMap::new();
                            let cols = row.columns();

                            for i in 0..row.len() {
                                let value = reflective_get(row, i);
                                vals.insert(cols.get(i).unwrap().name().to_string(), value);
                            }
                            vals
                        })
                        .collect::<Vec<HashMap<String, String>>>();

                    let list = rows
                        .into_iter()
                        .map(|row| FieldValue::owned_any(row.clone()));

                    Ok(Some(FieldValue::list(list)))
                })
            },
        ));

        for column in columns.iter() {
            q = q.field(Field::new(column, TypeRef::named(TypeRef::STRING), {
                let column = column.clone();
                move |ctx| {
                    let column = column.clone();
                    FieldFuture::new(async move {
                        let values = ctx
                            .parent_value
                            .downcast_ref::<HashMap<String, String>>()
                            .unwrap()
                            .clone();
                        let value = values.get(&column).unwrap().clone();
                        Ok(Some(Value::String(value)))
                    })
                }
            }));
        }

        qs.push(q);
    }

    let mut dyn_schema = dynamic::Schema::build(query.type_name(), None, None);

    for q in qs {
        dyn_schema = dyn_schema.register(q);
    }

    // let mutation = Object::new("Mutation");

    let dyn_schema = dyn_schema.register(query).data(client).finish()?;

    // let dyn_schema = dynamic::Schema::build(query, None, None);
    // let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();
    let app = Router::new().route("/", get(graphiql).post_service(GraphQL::new(dyn_schema)));

    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
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
        // "char" => {
        //     let v: i8 = row.get(index);
        // }
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
            let v: Option<SystemTime> = row.get(index);
            let v = DateTime::<chrono::Utc>::from(v.unwrap());
            Some(v.to_string())
        }
        &_ => Some("CANNOT PARSE".to_string()),
    };
    value.unwrap_or("null".to_string())
}
