use anyhow::Result;
use async_graphql::{
    dynamic::{Object, Schema},
    http::GraphiQLSource,
};
use async_graphql_axum::GraphQL;
use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use tokio::net::TcpListener;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod error;
mod graphql_schema;
mod postgres;
mod utils;

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

    let mut query = Object::new("Query");
    let mut dyn_schema = Schema::build(query.type_name(), None, None);

    // register interfaces for tables
    let interfaces = graphql_schema::generate_table_interfaces(tables.clone());
    for (_, interface) in interfaces {
        dyn_schema = dyn_schema.register(interface);
    }

    // register objects for tables
    let interfaces = graphql_schema::generate_table_interfaces(tables.clone());
    let objects =
        graphql_schema::generate_table_schemas(interfaces, tables.clone(), &client).await?;
    for object in objects {
        dyn_schema = dyn_schema.register(object);
    }

    // register query root
    let query_root = graphql_schema::generate_query_root(tables.clone());
    for field in query_root {
        query = query.field(field);
    }

    let dyn_schema = dyn_schema.register(query).data(client).finish()?;
    let app = Router::new().route("/", get(graphiql).post_service(GraphQL::new(dyn_schema)));
    let listener = TcpListener::bind("127.0.0.1:8000").await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
