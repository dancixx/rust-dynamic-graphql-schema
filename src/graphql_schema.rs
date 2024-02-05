use std::collections::BTreeMap;

use anyhow::Result;
use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, Interface, InterfaceField, Object, TypeRef},
    Value,
};
use tokio_postgres::Client;

use crate::{
    postgres::get_relations,
    utils::{col_type, reflective_get},
};

pub fn generate_query_root(tables: BTreeMap<String, Vec<(String, String)>>) -> Vec<Field> {
    let mut fields = Vec::with_capacity(tables.len());

    for (table_name, _) in tables {
        let field = Field::new(
            table_name.clone(),
            TypeRef::named_list_nn(table_name.clone()),
            move |ctx| {
                let table_name = table_name.clone();
                FieldFuture::new(async move {
                    let client = ctx.data::<tokio_postgres::Client>()?;
                    let field = ctx.field().selection_set();
                    let fields = field.into_iter().map(|f| f.name()).collect::<Vec<_>>();
                    let join_fields = fields
                        .iter()
                        .filter(|f| f.contains("_join_on_"))
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>();
                    let non_join_fields = fields
                        .iter()
                        .filter(|f| !f.contains("_join_on_"))
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>();
                    let non_join_fields = non_join_fields.join(", ");

                    let mut query_params = format!(
                        "SELECT {} FROM noexapp.{} LIMIT 100",
                        non_join_fields, table_name
                    );

                    // TODO: handle join fields
                    if !join_fields.is_empty() {
                        for join_field in join_fields {
                            let join_field = join_field.split("_join_on_").collect::<Vec<_>>();
                            let join_table = join_field[0];
                            let join_column = join_field[1];
                            query_params.push_str(&format!(
                                " JOIN noexapp.{} ON {}.{} = {}.{}",
                                join_table, table_name, join_column, join_table, join_column
                            ));
                        }
                    }

                    let query = client.query(&query_params, &[]).await?;
                    let rows = query
                        .iter()
                        .map(|row| {
                            let mut vals = BTreeMap::new();
                            let cols = row.columns();

                            for i in 0..row.len() {
                                let value = reflective_get(row, i);
                                vals.insert(cols.get(i).unwrap().name().to_string(), value);
                            }
                            vals
                        })
                        .collect::<Vec<BTreeMap<String, String>>>();

                    let list = rows
                        .into_iter()
                        .map(|row| FieldValue::owned_any(row.clone()));

                    Ok(Some(FieldValue::list(list)))
                })
            },
        );

        fields.push(field);
    }

    fields
}

pub async fn generate_table_schemas(
    interfaces: BTreeMap<String, Interface>,
    tables: BTreeMap<String, Vec<(String, String)>>,
    client: &Client,
) -> Result<Vec<Object>> {
    let mut objects = Vec::with_capacity(interfaces.len());
    let relations = get_relations(client).await.unwrap();

    for (table, interface) in interfaces {
        let mut object = Object::new(table.as_str()).implement(interface.type_name());
        let mut cols = tables.get(table.as_str()).unwrap().to_vec();
        let foreign_keys = relations.get(table.as_str());
        let foreign_keys = match foreign_keys {
            Some(fk) => fk
                .to_vec()
                .iter()
                .map(|f| (format!("{}_join_on_{}", f.0, f.1), f.1.to_string()))
                .collect(),
            None => vec![],
        };
        cols.extend(foreign_keys);
        cols.sort();

        for column in cols.iter() {
            object = object.field(Field::new(column.0.to_string(), col_type(&column.1), {
                let column = column.clone();
                move |ctx| {
                    let column = column.clone();
                    FieldFuture::new(async move {
                        let values = ctx
                            .parent_value
                            .downcast_ref::<BTreeMap<String, String>>()
                            .unwrap()
                            .clone();
                        let value = values.get(&column.0).unwrap().clone();
                        let col_type = col_type(&column.1);

                        if value == "NULL" {
                            return Ok(Some(Value::Null));
                        }

                        match col_type {
                            TypeRef::Named(name) => match name.as_ref() {
                                TypeRef::INT => Ok(Some(Value::Number(value.parse().unwrap()))),
                                TypeRef::FLOAT => Ok(Some(Value::Number(value.parse().unwrap()))),
                                TypeRef::ID => Ok(Some(Value::Number(value.parse().unwrap()))),
                                TypeRef::STRING => Ok(Some(Value::String(value))),
                                TypeRef::BOOLEAN => {
                                    Ok(Some(Value::Boolean(if value == String::from("true") {
                                        true
                                    } else {
                                        false
                                    })))
                                }
                                _ => Ok(Some(Value::String(value))),
                            },
                            _ => Ok(Some(Value::Null)),
                        }
                    })
                }
            }));
        }

        objects.push(object);
    }

    Ok(objects)
}

pub fn generate_table_interfaces(
    tables: BTreeMap<String, Vec<(String, String)>>,
) -> BTreeMap<String, Interface> {
    let mut interfaces = BTreeMap::new();

    for (table_name, cols) in tables {
        let mut interface = Interface::new(table_name.clone().to_uppercase());

        for col in cols {
            interface = interface.field(InterfaceField::new(col.0, col_type(&col.1)));
        }

        interfaces.insert(table_name, interface);
    }

    interfaces
}
