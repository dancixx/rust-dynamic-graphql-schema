use crate::postgres::reflective_get;
use anyhow::Result;
use async_graphql::{
    dynamic::{self, Field, FieldFuture, FieldValue, Object, TypeRef},
    http::GraphiQLSource,
    Value,
};
use async_graphql_axum::GraphQL;
use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use std::{collections::HashMap, error::Error};
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
    let tables = postgres::get_tables(&client).await?;
    let relations = postgres::get_relations(&client).await?;

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
                        "SELECT {} FROM noexapp.{} LIMIT 100;",
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
            q = q.field(Field::new(
                column.to_string(),
                TypeRef::named(TypeRef::STRING),
                {
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
                },
            ));
        }

        qs.push(q);
    }

    let mut dyn_schema = dynamic::Schema::build(query.type_name(), None, None);
    for q in qs {
        dyn_schema = dyn_schema.register(q);
    }

    let dyn_schema = dyn_schema.register(query).data(client).finish()?;
    let app = Router::new().route("/", get(graphiql).post_service(GraphQL::new(dyn_schema)));
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
