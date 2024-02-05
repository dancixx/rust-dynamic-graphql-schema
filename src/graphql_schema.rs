use std::collections::{BTreeMap, HashMap};

use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, Interface, InterfaceField, Object, TypeRef},
    Value,
};

use crate::utils::{col_type, reflective_get};

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
        );

        fields.push(field);
    }

    fields
}

pub fn generate_table_schemas(
    interfaces: BTreeMap<String, Interface>,
    tables: BTreeMap<String, Vec<(String, String)>>,
) -> Vec<Object> {
    let mut objects = Vec::with_capacity(interfaces.len());

    for (table, interface) in interfaces {
        let mut object = Object::new(table.as_str()).implement(interface.type_name());
        let cols = tables.get(table.as_str()).unwrap();
        for column in cols.iter() {
            object = object.field(Field::new(column.0.to_string(), col_type(&column.1), {
                let column = column.clone();
                move |ctx| {
                    let column = column.clone();
                    FieldFuture::new(async move {
                        let values = ctx
                            .parent_value
                            .downcast_ref::<HashMap<String, String>>()
                            .unwrap()
                            .clone();
                        let value = values.get(&column.0).unwrap().clone();
                        let col_type = col_type(&column.1);

                        match col_type {
                            TypeRef::Named(name) => match name.as_ref() {
                                TypeRef::INT => Ok(Some(Value::Number(value.parse().unwrap()))),
                                TypeRef::FLOAT => Ok(Some(Value::Number(value.parse().unwrap()))),
                                TypeRef::ID => Ok(Some(Value::String(value))),
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

    objects
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
