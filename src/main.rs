use crate::postgres::reflective_get;
use anyhow::Result;
use async_graphql::{
    dynamic::{self, Field, FieldFuture, FieldValue, Object, Schema, TypeRef},
    http::GraphiQLSource,
    Value,
};
use async_graphql_axum::GraphQL;
use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use std::collections::HashMap;
use tokio::net::TcpListener;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod error;
mod graphql_schema;
mod postgres;

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/").finish())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv()?;
    // Initialize the logger
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            Level::DEBUG,
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let client = postgres::connector().await?;
    println!("Connected to database");
    let tables = postgres::get_tables(&client).await?;
    let interfaces = graphql_schema::generate_table_interfaces(tables.clone());
    println!("{:?}", interfaces[0]);
    let relations = postgres::get_relations(&client).await?;

    let mut query = Object::new("Query");
    // let mut qs = Vec::new();

    // for (table_name, columns) in tables {
    //     let mut q = Object::new(&table_name);

    //     let table_name_clone = table_name.clone();
    //     query = query.field(Field::new(
    //         table_name.clone(),
    //         TypeRef::named_list_nn(q.type_name()),
    //         move |ctx| {
    //             let table_name = table_name_clone.clone();
    //             FieldFuture::new(async move {
    //                 let client = ctx.data::<tokio_postgres::Client>()?;
    //                 let field = ctx.field().selection_set();
    //                 let fields = field.into_iter().map(|f| f.name()).collect::<Vec<_>>();
    //                 let fields_to_string = fields.join(", ");

    //                 let query_params = format!(
    //                     "SELECT {} FROM noexapp.{} LIMIT 100;",
    //                     fields_to_string, table_name
    //                 );
    //                 let query = client.query(&query_params, &[]).await?;
    //                 let rows = query
    //                     .iter()
    //                     .map(|row| {
    //                         let mut vals = HashMap::new();
    //                         let cols = row.columns();

    //                         for i in 0..row.len() {
    //                             let value = reflective_get(row, i);
    //                             vals.insert(cols.get(i).unwrap().name().to_string(), value);
    //                         }
    //                         vals
    //                     })
    //                     .collect::<Vec<HashMap<String, String>>>();

    //                 let list = rows
    //                     .into_iter()
    //                     .map(|row| FieldValue::owned_any(row.clone()));

    //                 Ok(Some(FieldValue::list(list)))
    //             })
    //         },
    //     ));

    //     for column in columns.iter() {
    //         q = q.field(Field::new(
    //             column.to_string(),
    //             TypeRef::named(TypeRef::STRING),
    //             {
    //                 let column = column.clone();
    //                 move |ctx| {
    //                     let column = column.clone();
    //                     FieldFuture::new(async move {
    //                         let values = ctx
    //                             .parent_value
    //                             .downcast_ref::<HashMap<String, String>>()
    //                             .unwrap()
    //                             .clone();
    //                         let value = values.get(&column).unwrap().clone();
    //                         Ok(Some(Value::String(value)))
    //                     })
    //                 }
    //             },
    //         ));
    //     }

    //     qs.push(q);
    // }
    let mut dyn_schema = dynamic::Schema::build(query.type_name(), None, None);
    // for q in qs {
    //     let foreign_keys = relations.get(q.type_name());
    //     let foreign_keys = match foreign_keys {
    //         Some(foreign_keys) => {
    //             // handle duplicated foreign tables
    //             let mut foreign_keys = foreign_keys.clone();

    //             let mut counts = std::collections::HashMap::new();

    //             // Számold meg az egyes stringek előfordulásait
    //             for &(_, ref value) in &foreign_keys {
    //                 *counts.entry(value.clone()).or_insert(0) += 1;
    //             }

    //             // Hozzáfűz egy indexet azokhoz, amelyek többször szerepelnek
    //             let mut indices = std::collections::HashMap::new();
    //             for &mut (ref key, ref mut value) in &mut foreign_keys {
    //                 if counts[value] > 1 {
    //                     let count = indices.entry(value.clone()).or_insert(1);
    //                     *value = format!("{}__{}", value, count);
    //                     *count += 1;
    //                 }
    //             }

    //             foreign_keys
    //         }
    //         None => {
    //             dyn_schema = dyn_schema.register(q);
    //             continue;
    //         }
    //     };

    //     let mut q = q;
    //     println!("{:?}: {:?}", q.type_name(), foreign_keys);

    //     for (f_col, f_table) in foreign_keys {
    //         // split f_table by :
    //         let f_table_split = f_table.clone();
    //         let f_table_split = f_table_split.split("__").collect::<Vec<&str>>();
    //         let f_table_split = f_table_split[0].to_string();
    //         q = q.field(Field::new(
    //             f_table.to_string(),
    //             TypeRef::named(f_table_split.to_string()),
    //             {
    //                 let f_table = f_table.clone();
    //                 let f_col = f_col.clone();
    //                 move |ctx| {
    //                     let f_table = f_table.clone();
    //                     let f_col = f_col.clone();
    //                     FieldFuture::new(async move {
    //                         let client = ctx.data::<tokio_postgres::Client>()?;
    //                         let parent_value = ctx.parent_value;
    //                         let parent_value = parent_value
    //                             .downcast_ref::<HashMap<String, String>>()
    //                             .unwrap()
    //                             .clone();
    //                         let parent_value = parent_value.get(&f_col).unwrap().clone();
    //                         let query_params = format!(
    //                             "SELECT * FROM noexapp.{} WHERE {} = '{}';",
    //                             f_table, f_col, parent_value
    //                         );
    //                         let query = client.query(&query_params, &[]).await?;
    //                         let rows = query
    //                             .iter()
    //                             .map(|row| {
    //                                 let mut vals = HashMap::new();
    //                                 let cols = row.columns();

    //                                 for i in 0..row.len() {
    //                                     let value = reflective_get(row, i);
    //                                     vals.insert(cols.get(i).unwrap().name().to_string(), value);
    //                                 }
    //                                 vals
    //                             })
    //                             .collect::<Vec<HashMap<String, String>>>();

    //                         let list = rows
    //                             .into_iter()
    //                             .map(|row| FieldValue::owned_any(row.clone()));

    //                         Ok(Some(FieldValue::list(list)))
    //                     })
    //                 }
    //             },
    //         ))
    //     }

    //     dyn_schema = dyn_schema.register(q);
    // }

    let dyn_schema = dyn_schema.register(query).data(client).finish()?;
    let app = Router::new().route("/", get(graphiql).post_service(GraphQL::new(dyn_schema)));
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
