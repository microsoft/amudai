use crate::{
    projection::{SchemaProjection, SchemaProjectionBuilder},
    schema::{BasicType, DataType, SchemaId},
    schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder},
};

#[test]
fn test_filtered_schema_projection() {
    let schema = populate_test_schema().into_schema().unwrap();

    let projected = SchemaProjection::filtered(schema.clone(), |dt| {
        let ty = dt.basic_type().unwrap();
        if ty.is_composite() {
            return None;
        }
        Some(ty == BasicType::Int64)
    });
    assert!(projected.to_string().contains("api_endpoints: map"));

    let projected = SchemaProjection::filtered(schema.clone(), |dt| {
        let ty = dt.basic_type().unwrap();
        Some(ty == BasicType::Int64)
    });
    assert_eq!(projected.fields().len(), 1);
    assert_eq!(projected.fields()[0].name().as_ref(), "id");
}

#[test]
fn test_schema_projection_builder() {
    let schema = populate_test_schema().into_schema().unwrap();

    // Test 1: Empty builder creates empty projection
    let builder = SchemaProjectionBuilder::new(schema.clone());
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 0);
    assert!(projection.fields().is_empty());

    // Test 2: Add simple top-level field
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder.add_path(&["name"]).unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);
    assert_eq!(projection.fields()[0].name().as_ref(), "name");
    assert_eq!(projection.fields()[0].basic_type(), BasicType::String);

    // Test 3: Add multiple simple fields
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder.add_path(&["id"]).unwrap();
    builder.add_path(&["name"]).unwrap();
    builder.add_path(&["price"]).unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 3);

    // Verify field names and types
    let field_names: Vec<&str> = projection
        .fields()
        .iter()
        .map(|f| f.name().as_ref())
        .collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"price"));

    // Test 4: Add nested field path (maintains structural integrity)
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder.add_path(&["user_profile", "email"]).unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);

    let user_profile_field = &projection.fields()[0];
    assert_eq!(user_profile_field.name().as_ref(), "user_profile");
    assert_eq!(user_profile_field.basic_type(), BasicType::Struct);
    assert_eq!(user_profile_field.children().len(), 1);
    assert_eq!(user_profile_field.children()[0].name().as_ref(), "email");

    // Test 5: Add deeply nested path (3 levels)
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder
        .add_path(&["user_profile", "address", "city"])
        .unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);

    let user_profile = &projection.fields()[0];
    assert_eq!(user_profile.children().len(), 1);

    let address = &user_profile.children()[0];
    assert_eq!(address.name().as_ref(), "address");
    assert_eq!(address.children().len(), 1);

    let city = &address.children()[0];
    assert_eq!(city.name().as_ref(), "city");
    assert_eq!(city.basic_type(), BasicType::String);

    // Test 6: Add multiple paths that share common ancestors
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder
        .add_path(&["user_profile", "address", "city"])
        .unwrap();
    builder
        .add_path(&["user_profile", "address", "country"])
        .unwrap();
    builder.add_path(&["user_profile", "email"]).unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);

    let user_profile = &projection.fields()[0];
    assert_eq!(user_profile.children().len(), 2); // address and email

    // Find address field
    let address = user_profile
        .children()
        .iter()
        .find(|f| f.name().as_ref() == "address")
        .unwrap();
    assert_eq!(address.children().len(), 2); // city and country

    let child_names: Vec<&str> = address
        .children()
        .iter()
        .map(|f| f.name().as_ref())
        .collect();
    assert!(child_names.contains(&"city"));
    assert!(child_names.contains(&"country"));

    // Find email field
    let email = user_profile
        .children()
        .iter()
        .find(|f| f.name().as_ref() == "email")
        .unwrap();
    assert_eq!(email.basic_type(), BasicType::String);

    // Test 7: Add path through list
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder.add_path(&["orders", "item", "order_id"]).unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);

    let orders = &projection.fields()[0];
    assert_eq!(orders.name().as_ref(), "orders");
    assert_eq!(orders.basic_type(), BasicType::List);
    assert_eq!(orders.children().len(), 1);

    let order_item = &orders.children()[0];
    assert_eq!(order_item.name().as_ref(), "item");
    assert_eq!(order_item.basic_type(), BasicType::Struct);
    assert_eq!(order_item.children().len(), 1);

    let order_id = &order_item.children()[0];
    assert_eq!(order_id.name().as_ref(), "order_id");
    assert_eq!(order_id.basic_type(), BasicType::String);

    // Test 8: Add path through map
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder
        .add_path(&["metadata", "value", "string_value"])
        .unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);

    let metadata = &projection.fields()[0];
    assert_eq!(metadata.name().as_ref(), "metadata");
    assert_eq!(metadata.basic_type(), BasicType::Map);
    assert_eq!(metadata.children().len(), 1); // Only value child is included

    let value = &metadata.children()[0];
    assert_eq!(value.name().as_ref(), "value");
    assert_eq!(value.basic_type(), BasicType::Struct);
    assert_eq!(value.children().len(), 1);

    let string_value = &value.children()[0];
    assert_eq!(string_value.name().as_ref(), "string_value");
    assert_eq!(string_value.basic_type(), BasicType::String);

    // Test 9: Add very deeply nested path (4+ levels)
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder
        .add_path(&[
            "configuration",
            "database",
            "connection_pool",
            "retry_policy",
            "max_retries",
        ])
        .unwrap();
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 1);

    let config = &projection.fields()[0];
    assert_eq!(config.name().as_ref(), "configuration");
    let database = &config.children()[0];
    assert_eq!(database.name().as_ref(), "database");
    let pool = &database.children()[0];
    assert_eq!(pool.name().as_ref(), "connection_pool");
    let retry = &pool.children()[0];
    assert_eq!(retry.name().as_ref(), "retry_policy");
    let max_retries = &retry.children()[0];
    assert_eq!(max_retries.name().as_ref(), "max_retries");
    assert_eq!(max_retries.basic_type(), BasicType::Int32);

    // Test 10: Error cases - invalid paths
    let mut builder = SchemaProjectionBuilder::new(schema.clone());

    // Non-existent top-level field
    assert!(builder.add_path(&["nonexistent"]).is_err());

    // Non-existent nested field
    assert!(builder.add_path(&["user_profile", "nonexistent"]).is_err());

    // Path through primitive field (invalid)
    assert!(builder.add_path(&["name", "invalid_child"]).is_err());

    // Test 11: Complex mixed paths
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder.add_path(&["id"]).unwrap();
    builder
        .add_path(&["user_profile", "address", "coordinates", "latitude"])
        .unwrap();
    builder
        .add_path(&[
            "orders",
            "item",
            "line_items",
            "item",
            "product_details",
            "name",
        ])
        .unwrap();
    builder.add_path(&["metadata", "key"]).unwrap();
    builder
        .add_path(&["sensor_readings", "", "measurements", "value", "raw_value"])
        .unwrap();

    let projection = builder.build();
    assert_eq!(projection.fields().len(), 5);

    // Verify all top-level fields are present
    let top_level_names: Vec<&str> = projection
        .fields()
        .iter()
        .map(|f| f.name().as_ref())
        .collect();
    assert!(top_level_names.contains(&"id"));
    assert!(top_level_names.contains(&"user_profile"));
    assert!(top_level_names.contains(&"orders"));
    assert!(top_level_names.contains(&"metadata"));
    assert!(top_level_names.contains(&"sensor_readings"));

    let measurements = projection
        .fields()
        .find("sensor_readings")
        .unwrap()
        .children()[0]
        .children()
        .find("measurements")
        .unwrap();
    assert_eq!(measurements.basic_type(), BasicType::Map);

    // Test 12: Empty path handling
    let mut builder = SchemaProjectionBuilder::new(schema.clone());
    builder.add_path(Vec::<&str>::new()).unwrap(); // Empty path should be no-op
    let projection = builder.build();
    assert_eq!(projection.fields().len(), 0);
}

#[test]
fn test_basic_schema_building() {
    let mut schema = SchemaBuilder::new(vec![]);
    schema.add_field(FieldBuilder::new_i64().with_name("id"));
    schema.add_field(FieldBuilder::new_str().with_name("source"));
    schema.add_field(FieldBuilder::new_struct().with_name("props"));

    for i in 0..20 {
        let name = format!("prop{i}");
        schema
            .find_field_mut("props")
            .unwrap()
            .add_child(DataTypeBuilder::new_str().with_field_name(name));
    }

    let schema = schema.into_schema().unwrap();
    assert_eq!(schema.len().unwrap(), 3);
    assert_eq!(schema.field_at(1).unwrap().name().unwrap(), "source");

    assert_eq!(
        schema
            .find_field("source")
            .unwrap()
            .unwrap()
            .1
            .name()
            .unwrap(),
        "source"
    );

    let field = schema.field_at(2).unwrap();
    let dt = field.data_type().unwrap();
    assert_eq!(dt.name().unwrap(), "props");
    assert_eq!(dt.basic_type().unwrap(), BasicType::Struct);
    assert_eq!(dt.child_at(0).unwrap().name().unwrap(), "prop0");
    assert!(dt.parent_schema_id().unwrap().is_none());

    let child = dt.find_child("prop10").unwrap().unwrap().1;
    assert_eq!(child.name().unwrap(), "prop10");
    assert_eq!(
        child.parent_schema_id().unwrap(),
        Some(dt.schema_id().unwrap())
    );
    assert!(dt.find_child("prop30").unwrap().is_none());
}

#[test]
fn test_parent_schema_ids() {
    fn check_parent(parent: SchemaId, node: DataType) {
        let actual_parent = node
            .parent_schema_id()
            .unwrap()
            .unwrap_or(SchemaId::invalid());
        assert_eq!(actual_parent, parent);
        let schema_id = node.schema_id().unwrap();
        for child in node.field_list().unwrap() {
            check_parent(schema_id, child.unwrap());
        }
    }

    let schema = populate_test_schema();
    let schema_message = schema.finish_and_seal();
    let schema = schema_message.schema().unwrap();

    for field in schema.field_list().unwrap() {
        let field = field.unwrap();
        check_parent(SchemaId::invalid(), field);
    }
}

#[test]
fn test_resolve_schema_ids() {
    let schema = populate_test_schema();
    let schema_message = schema.finish_and_seal();
    let schema = schema_message.schema().unwrap();

    for id in 0..schema.next_schema_id().unwrap().as_u32() {
        let id = SchemaId::from(id);
        let data_type = schema.resolve_schema_id(id).unwrap().unwrap();
        assert_eq!(data_type.schema_id().unwrap(), id);
    }
}

#[test]
fn test_resolve_field_path() {
    let schema = populate_test_schema().into_schema().unwrap();

    // Test empty path - should return empty result
    let result = schema.resolve_field_path(Vec::<&str>::new()).unwrap();
    assert!(result.is_empty());

    // Test valid simple field path
    let result = schema.resolve_field_path(&["name"]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name().unwrap(), "name");

    // Test valid nested struct path (3 levels deep)
    let result = schema
        .resolve_field_path(&["user_profile", "address", "city"])
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].name().unwrap(), "user_profile");
    assert_eq!(result[1].name().unwrap(), "address");
    assert_eq!(result[2].name().unwrap(), "city");

    // Test valid deeply nested path (4 levels deep)
    let result = schema
        .resolve_field_path(&["user_profile", "address", "coordinates", "latitude"])
        .unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[3].name().unwrap(), "latitude");

    // Test non-existent top-level field
    assert!(schema.resolve_field_path(&["nonexistent"]).is_err());

    // Test non-existent field in the middle of path
    assert!(
        schema
            .resolve_field_path(&["user_profile", "nonexistent", "city"])
            .is_err()
    );

    // Test non-existent leaf field
    assert!(
        schema
            .resolve_field_path(&["user_profile", "address", "nonexistent"])
            .is_err()
    );

    // Test path through list to struct field
    let result = schema
        .resolve_field_path(&["orders", "item", "order_id"])
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[1].name().unwrap(), "item"); // list item
    assert_eq!(result[2].name().unwrap(), "order_id");

    // Test path through list to deeply nested field
    let result = schema
        .resolve_field_path(&[
            "orders",
            "item",
            "line_items",
            "item",
            "product_details",
            "name",
        ])
        .unwrap();
    assert_eq!(result.len(), 6);
    assert_eq!(result[5].name().unwrap(), "name");

    // Test path through map using "key" accessor
    let result = schema.resolve_field_path(&["metadata", "key"]).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[1].name().unwrap(), "key");

    // Test path through map using "value" accessor
    let result = schema.resolve_field_path(&["metadata", "value"]).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[1].name().unwrap(), "value");

    // Test path through map to nested field in value
    let result = schema
        .resolve_field_path(&["metadata", "value", "string_value"])
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[2].name().unwrap(), "string_value");

    // Test path through sensor readings map
    let result = schema
        .resolve_field_path(&[
            "sensor_readings",
            "item",
            "measurements",
            "value",
            "raw_value",
        ])
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[4].name().unwrap(), "raw_value");

    // Test deep nested path (4+ levels)
    let result = schema
        .resolve_field_path(&[
            "configuration",
            "database",
            "connection_pool",
            "retry_policy",
            "max_retries",
        ])
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[4].name().unwrap(), "max_retries");

    // Test path through deeply nested list
    let result = schema
        .resolve_field_path(&[
            "configuration",
            "database",
            "connection_pool",
            "retry_policy",
            "retry_conditions",
            "item",
        ])
        .unwrap();
    assert_eq!(result.len(), 6);
    assert_eq!(result[5].name().unwrap(), "item");

    let result = schema
        .resolve_field_path(&[
            "configuration",
            "api_endpoints",
            "value",
            "authentication",
            "method",
        ])
        .unwrap();
    assert_eq!(result.len(), 5);
    assert_eq!(result[4].name().unwrap(), "method");

    // Test invalid path through primitive field (trying to access child of non-composite type)
    assert!(
        schema
            .resolve_field_path(&["name", "invalid_child"])
            .is_err()
    );

    // Test invalid path with non-existent field in complex nested structure
    assert!(
        schema
            .resolve_field_path(&["user_profile", "address", "coordinates", "nonexistent"])
            .is_err()
    );
}

#[test]
fn test_complex_nested_schema() {
    let schema = populate_test_schema();
    let schema_message = schema.finish_and_seal();
    let schema = schema_message.schema().unwrap();

    // Verify we have 10 top-level fields
    assert_eq!(schema.len().unwrap(), 10);

    // Test simple fields
    assert_eq!(schema.field_at(0).unwrap().name().unwrap(), "id");
    assert_eq!(schema.field_at(1).unwrap().name().unwrap(), "name");

    // Test complex nested field: user_profile
    let user_profile_field = schema.field_at(4).unwrap();
    assert_eq!(user_profile_field.name().unwrap(), "user_profile");
    let user_profile_dt = user_profile_field.data_type().unwrap();
    assert_eq!(user_profile_dt.basic_type().unwrap(), BasicType::Struct);

    // Verify nested address structure (level 2)
    let address = user_profile_dt.find_child("address").unwrap().unwrap().1;
    assert_eq!(address.basic_type().unwrap(), BasicType::Struct);

    // Verify deeply nested coordinates (level 3)
    let coordinates = address.find_child("coordinates").unwrap().unwrap().1;
    assert_eq!(coordinates.basic_type().unwrap(), BasicType::Struct);
    assert!(coordinates.find_child("latitude").unwrap().is_some());
    assert!(coordinates.find_child("longitude").unwrap().is_some());
    assert!(coordinates.find_child("elevation").unwrap().is_some());

    // Test list field: orders
    let orders_field = schema.field_at(5).unwrap();
    assert_eq!(orders_field.name().unwrap(), "orders");
    let orders_dt = orders_field.data_type().unwrap();
    assert_eq!(orders_dt.basic_type().unwrap(), BasicType::List);

    // Verify nested structure within list items
    let order_item = orders_dt.child_at(0).unwrap();
    assert_eq!(order_item.basic_type().unwrap(), BasicType::Struct);

    // Test map field: metadata
    let metadata_field = schema.field_at(6).unwrap();
    assert_eq!(metadata_field.name().unwrap(), "metadata");
    let metadata_dt = metadata_field.data_type().unwrap();
    assert_eq!(metadata_dt.basic_type().unwrap(), BasicType::Map);

    // Verify map has key and value children
    assert_eq!(metadata_dt.child_count().unwrap(), 2);
    let key_child = metadata_dt.child_at(0).unwrap();
    let value_child = metadata_dt.child_at(1).unwrap();
    assert_eq!(key_child.name().unwrap(), "key");
    assert_eq!(value_child.name().unwrap(), "value");
    assert_eq!(value_child.basic_type().unwrap(), BasicType::Struct);

    // Test deeply nested sensor_readings (4 levels deep)
    let sensor_readings_field = schema.field_at(8).unwrap();
    assert_eq!(sensor_readings_field.name().unwrap(), "sensor_readings");
    let sensor_readings_dt = sensor_readings_field.data_type().unwrap();
    assert_eq!(sensor_readings_dt.basic_type().unwrap(), BasicType::List);

    let reading_item = sensor_readings_dt.child_at(0).unwrap();
    let measurements = reading_item.find_child("measurements").unwrap().unwrap().1;
    assert_eq!(measurements.basic_type().unwrap(), BasicType::Map);

    let measurement_value = measurements.child_at(1).unwrap(); // value child
    let quality_metrics = measurement_value
        .find_child("quality_metrics")
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(quality_metrics.basic_type().unwrap(), BasicType::Struct);

    // Verify 4th level nested list
    let validation_flags = quality_metrics
        .find_child("validation_flags")
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(validation_flags.basic_type().unwrap(), BasicType::List);

    // Test configuration field with 4 levels of nesting
    let config_field = schema.field_at(9).unwrap();
    assert_eq!(config_field.name().unwrap(), "configuration");
    let config_dt = config_field.data_type().unwrap();

    let db_config = config_dt.find_child("database").unwrap().unwrap().1;
    let pool_config = db_config.find_child("connection_pool").unwrap().unwrap().1;
    let retry_policy = pool_config.find_child("retry_policy").unwrap().unwrap().1;
    let retry_conditions = retry_policy
        .find_child("retry_conditions")
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(retry_conditions.basic_type().unwrap(), BasicType::List);
}

fn populate_test_schema() -> SchemaBuilder {
    let mut schema = SchemaBuilder::new(vec![]);

    // 1. Simple ID field
    schema.add_field(FieldBuilder::new_i64().with_name("id"));

    // 2. Simple name field
    schema.add_field(FieldBuilder::new_str().with_name("name"));

    // 3. Timestamp field
    schema.add_field(
        DataTypeBuilder::new_datetime()
            .with_field_name("created_at")
            .into(),
    );

    // 4. Decimal price field
    schema.add_field(
        DataTypeBuilder::new_decimal()
            .with_field_name("price")
            .into(),
    );

    // 5. Complex nested struct: User profile with address and preferences
    let mut user_profile = FieldBuilder::new_struct().with_name("user_profile");

    // Level 2: User basic info
    user_profile.add_child(DataTypeBuilder::new_str().with_field_name("email"));
    user_profile.add_child(DataTypeBuilder::new(
        "age",
        BasicType::Int32,
        true,
        None,
        vec![],
    ));

    // Level 2: Nested address struct
    let mut address = DataTypeBuilder::new("address", BasicType::Struct, None, None, vec![]);
    address.add_child(DataTypeBuilder::new_str().with_field_name("street"));
    address.add_child(DataTypeBuilder::new_str().with_field_name("city"));
    address.add_child(DataTypeBuilder::new_str().with_field_name("country"));
    address.add_child(DataTypeBuilder::new(
        "zip_code",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));

    // Level 3: Nested coordinates struct within address
    let mut coordinates =
        DataTypeBuilder::new("coordinates", BasicType::Struct, None, None, vec![]);
    coordinates.add_child(DataTypeBuilder::new(
        "latitude",
        BasicType::Float64,
        true,
        None,
        vec![],
    ));
    coordinates.add_child(DataTypeBuilder::new(
        "longitude",
        BasicType::Float64,
        true,
        None,
        vec![],
    ));
    coordinates.add_child(DataTypeBuilder::new(
        "elevation",
        BasicType::Float32,
        true,
        None,
        vec![],
    ));
    address.add_child(coordinates);

    user_profile.add_child(address);

    // Level 2: User preferences with nested lists
    let mut preferences =
        DataTypeBuilder::new("preferences", BasicType::Struct, None, None, vec![]);

    // Level 3: List of favorite categories (strings)
    let mut favorite_categories =
        DataTypeBuilder::new("favorite_categories", BasicType::List, None, None, vec![]);
    favorite_categories.add_child(DataTypeBuilder::new_str().with_field_name("item"));
    preferences.add_child(favorite_categories);

    // Level 3: List of notification settings (nested structs)
    let mut notification_settings =
        DataTypeBuilder::new("notification_settings", BasicType::List, None, None, vec![]);
    let mut notification_item = DataTypeBuilder::new("item", BasicType::Struct, None, None, vec![]);
    notification_item.add_child(DataTypeBuilder::new_str().with_field_name("type"));
    notification_item.add_child(DataTypeBuilder::new(
        "enabled",
        BasicType::Boolean,
        None,
        None,
        vec![],
    ));
    notification_item.add_child(DataTypeBuilder::new(
        "frequency",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));
    notification_settings.add_child(notification_item);
    preferences.add_child(notification_settings);

    user_profile.add_child(preferences);
    schema.add_field(user_profile);

    // 6. List of orders with nested complexity (3-4 levels deep)
    let mut orders = FieldBuilder::new_list().with_name("orders");
    let mut order_item = DataTypeBuilder::new("item", BasicType::Struct, None, None, vec![]);
    order_item.add_child(DataTypeBuilder::new_str().with_field_name("order_id"));
    order_item.add_child(DataTypeBuilder::new_datetime().with_field_name("order_date"));
    order_item.add_child(DataTypeBuilder::new_decimal().with_field_name("total_amount"));

    // Level 3: List of line items within each order
    let mut line_items = DataTypeBuilder::new("line_items", BasicType::List, None, None, vec![]);
    let mut line_item = DataTypeBuilder::new("item", BasicType::Struct, None, None, vec![]);
    line_item.add_child(DataTypeBuilder::new_str().with_field_name("product_id"));
    line_item.add_child(DataTypeBuilder::new(
        "quantity",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));
    line_item.add_child(DataTypeBuilder::new_decimal().with_field_name("unit_price"));

    // Level 4: Product details struct within line items
    let mut product_details =
        DataTypeBuilder::new("product_details", BasicType::Struct, None, None, vec![]);
    product_details.add_child(DataTypeBuilder::new_str().with_field_name("name"));
    product_details.add_child(DataTypeBuilder::new_str().with_field_name("category"));
    product_details.add_child(DataTypeBuilder::new(
        "weight",
        BasicType::Float32,
        true,
        None,
        vec![],
    ));

    // Level 4: List of tags within product details
    let mut tags = DataTypeBuilder::new("tags", BasicType::List, None, None, vec![]);
    tags.add_child(DataTypeBuilder::new_str().with_field_name("item"));
    product_details.add_child(tags);

    line_item.add_child(product_details);
    line_items.add_child(line_item);
    order_item.add_child(line_items);

    orders.add_child(order_item);
    schema.add_field(orders);

    // 7. Map field: metadata key-value pairs with nested values
    let mut metadata = FieldBuilder::new("metadata", BasicType::Map, None, None);
    // Map key type (string)
    metadata.add_child(DataTypeBuilder::new_str().with_field_name("key"));

    // Map value type (nested struct with various data types)
    let mut metadata_value = DataTypeBuilder::new("value", BasicType::Struct, None, None, vec![]);
    metadata_value.add_child(DataTypeBuilder::new_str().with_field_name("string_value"));
    metadata_value.add_child(DataTypeBuilder::new(
        "numeric_value",
        BasicType::Float64,
        true,
        None,
        vec![],
    ));
    metadata_value.add_child(DataTypeBuilder::new(
        "boolean_value",
        BasicType::Boolean,
        None,
        None,
        vec![],
    ));

    // Level 3: Nested list within map values
    let mut nested_list =
        DataTypeBuilder::new("nested_values", BasicType::List, None, None, vec![]);
    let mut nested_item = DataTypeBuilder::new("item", BasicType::Struct, None, None, vec![]);
    nested_item.add_child(DataTypeBuilder::new_str().with_field_name("label"));
    nested_item.add_child(DataTypeBuilder::new(
        "score",
        BasicType::Float32,
        true,
        None,
        vec![],
    ));
    nested_list.add_child(nested_item);
    metadata_value.add_child(nested_list);

    metadata.add_child(metadata_value);
    schema.add_field(metadata);

    // 8. Dynamic field for flexible JSON-like data
    schema.add_field(
        DataTypeBuilder::new_dynamic()
            .with_field_name("properties")
            .into(),
    );

    // 9. List of deeply nested sensor readings
    let mut sensor_readings = FieldBuilder::new_list().with_name("sensor_readings");
    let mut reading = DataTypeBuilder::new("item", BasicType::Struct, None, None, vec![]);
    reading.add_child(DataTypeBuilder::new_str().with_field_name("sensor_id"));
    reading.add_child(DataTypeBuilder::new_datetime().with_field_name("timestamp"));

    // Level 3: Measurements map
    let mut measurements = DataTypeBuilder::new("measurements", BasicType::Map, None, None, vec![]);
    measurements.add_child(DataTypeBuilder::new_str().with_field_name("key")); // measurement type

    // Level 3: Measurement value with statistics
    let mut measurement_value =
        DataTypeBuilder::new("value", BasicType::Struct, None, None, vec![]);
    measurement_value.add_child(DataTypeBuilder::new(
        "raw_value",
        BasicType::Float64,
        true,
        None,
        vec![],
    ));
    measurement_value.add_child(DataTypeBuilder::new(
        "calibrated_value",
        BasicType::Float64,
        true,
        None,
        vec![],
    ));
    measurement_value.add_child(DataTypeBuilder::new_str().with_field_name("unit"));

    // Level 4: Quality metrics struct
    let mut quality_metrics =
        DataTypeBuilder::new("quality_metrics", BasicType::Struct, None, None, vec![]);
    quality_metrics.add_child(DataTypeBuilder::new(
        "confidence",
        BasicType::Float32,
        true,
        None,
        vec![],
    ));
    quality_metrics.add_child(DataTypeBuilder::new(
        "accuracy",
        BasicType::Float32,
        true,
        None,
        vec![],
    ));
    quality_metrics.add_child(DataTypeBuilder::new(
        "error_margin",
        BasicType::Float32,
        true,
        None,
        vec![],
    ));

    // Level 4: List of validation flags
    let mut validation_flags =
        DataTypeBuilder::new("validation_flags", BasicType::List, None, None, vec![]);
    validation_flags.add_child(DataTypeBuilder::new_str().with_field_name("item"));
    quality_metrics.add_child(validation_flags);

    measurement_value.add_child(quality_metrics);
    measurements.add_child(measurement_value);
    reading.add_child(measurements);

    sensor_readings.add_child(reading);
    schema.add_field(sensor_readings);

    // 10. Complex configuration struct with multiple nested levels
    let mut config = FieldBuilder::new_struct().with_name("configuration");

    // Level 2: Database connection settings
    let mut db_config = DataTypeBuilder::new("database", BasicType::Struct, None, None, vec![]);
    db_config.add_child(DataTypeBuilder::new_str().with_field_name("host"));
    db_config.add_child(DataTypeBuilder::new(
        "port",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));
    db_config.add_child(DataTypeBuilder::new_str().with_field_name("database_name"));

    // Level 3: Connection pool settings
    let mut pool_config =
        DataTypeBuilder::new("connection_pool", BasicType::Struct, None, None, vec![]);
    pool_config.add_child(DataTypeBuilder::new(
        "min_connections",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));
    pool_config.add_child(DataTypeBuilder::new(
        "max_connections",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));
    pool_config.add_child(DataTypeBuilder::new_timespan().with_field_name("timeout"));

    // Level 4: Retry policy within pool config
    let mut retry_policy =
        DataTypeBuilder::new("retry_policy", BasicType::Struct, None, None, vec![]);
    retry_policy.add_child(DataTypeBuilder::new(
        "max_retries",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));
    retry_policy.add_child(DataTypeBuilder::new_timespan().with_field_name("base_delay"));
    retry_policy.add_child(DataTypeBuilder::new(
        "exponential_backoff",
        BasicType::Boolean,
        None,
        None,
        vec![],
    ));

    // Level 4: List of retry conditions
    let mut retry_conditions =
        DataTypeBuilder::new("retry_conditions", BasicType::List, None, None, vec![]);
    retry_conditions.add_child(DataTypeBuilder::new_str().with_field_name("item"));
    retry_policy.add_child(retry_conditions);

    pool_config.add_child(retry_policy);
    db_config.add_child(pool_config);
    config.add_child(db_config);

    // Level 2: API endpoints configuration
    let mut api_config = DataTypeBuilder::new("api_endpoints", BasicType::Map, None, None, vec![]);
    api_config.add_child(DataTypeBuilder::new_str().with_field_name("key")); // endpoint name

    // Level 3: Endpoint configuration struct
    let mut endpoint_config = DataTypeBuilder::new("value", BasicType::Struct, None, None, vec![]);
    endpoint_config.add_child(DataTypeBuilder::new_str().with_field_name("url"));
    endpoint_config.add_child(DataTypeBuilder::new_timespan().with_field_name("timeout"));
    endpoint_config.add_child(DataTypeBuilder::new(
        "retries",
        BasicType::Int32,
        false,
        None,
        vec![],
    ));

    // Level 4: Authentication settings
    let mut auth_config =
        DataTypeBuilder::new("authentication", BasicType::Struct, None, None, vec![]);
    auth_config.add_child(DataTypeBuilder::new_str().with_field_name("method"));
    auth_config.add_child(DataTypeBuilder::new_str().with_field_name("token"));
    auth_config.add_child(DataTypeBuilder::new_timespan().with_field_name("token_expiry"));

    // Level 4: List of required scopes
    let mut scopes = DataTypeBuilder::new("scopes", BasicType::List, None, None, vec![]);
    scopes.add_child(DataTypeBuilder::new_str().with_field_name("item"));
    auth_config.add_child(scopes);

    endpoint_config.add_child(auth_config);
    api_config.add_child(endpoint_config);
    config.add_child(api_config);

    schema.add_field(config);

    schema
}
