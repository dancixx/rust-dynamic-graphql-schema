use std::collections::BTreeMap;

use async_graphql::dynamic::{Interface, InterfaceField, TypeRef};

pub fn generate_table_interfaces(
    tables: BTreeMap<String, Vec<(String, String)>>,
) -> Vec<Interface> {
    let mut interfaces = Vec::new();

    for (table_name, cols) in tables {
        let mut interface = Interface::new(table_name);

        for col in cols {
            interface = interface.field(InterfaceField::new(col.0, col_type(&col.1)));
        }

        interfaces.push(interface);
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
