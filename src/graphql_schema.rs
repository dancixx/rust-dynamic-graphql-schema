use std::collections::BTreeMap;

use anyhow::Ok;
use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, Interface, InterfaceField, Object, TypeRef},
    Value,
};

use crate::postgres::reflective_get;

// pub fn generate_query_dyn_schema(
//     query: &mut Object,
//     tables: BTreeMap<String, Vec<(String, String)>>,
// ) {

// }

pub fn generate_table_interfaces(
    tables: BTreeMap<String, Vec<(String, String)>>,
) -> BTreeMap<String, Interface> {
    let mut interfaces = BTreeMap::new();

    for (table_name, cols) in tables {
        let mut interface = Interface::new(table_name.clone());

        for col in cols {
            interface = interface.field(InterfaceField::new(col.0, col_type(&col.1)));
        }

        interfaces.insert(table_name, interface);
    }

    interfaces
}

pub fn col_type(pg_type: &str) -> TypeRef {
    match pg_type {
        "bool" => TypeRef::named(TypeRef::BOOLEAN),
        "varchar" | "char(n)" | "text" | "name" => TypeRef::named(TypeRef::STRING),
        "char" => TypeRef::named(TypeRef::STRING),
        "int2" | "smallserial" | "smallint" => TypeRef::named(TypeRef::INT),
        "int" | "int4" | "serial" => TypeRef::named(TypeRef::INT),
        "int8" | "bigserial" | "bigint" => TypeRef::named(TypeRef::INT),
        "float4" | "real" => TypeRef::named(TypeRef::FLOAT),
        "float8" | "double precision" => TypeRef::named(TypeRef::FLOAT),
        "timestamp" | "timestamptz" => TypeRef::named(TypeRef::STRING),
        _ => TypeRef::named(TypeRef::STRING),
    }
}
