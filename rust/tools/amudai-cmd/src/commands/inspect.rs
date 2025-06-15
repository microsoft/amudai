//! Inspect command implementation

use amudai_format::defs::{common::DataRef, shard::ShardDirectory};
use amudai_objectstore::{ObjectStore, local_store::LocalFsObjectStore, url::ObjectUrl};
use amudai_shard::read::shard::ShardOptions;
use anyhow::{Context, Result};
use serde::Serialize;
use std::sync::Arc;

use crate::commands::file_path_to_object_url;

#[derive(Serialize)]
struct InspectSummary {
    shard_directory: ShardDirectoryInfo,
    schema: SchemaInfo,
    stripes: Vec<StripeInfo>,
    url_list: UrlListInfo,
}

#[derive(Serialize)]
struct ShardDirectoryInfo {
    total_record_count: u64,
    deleted_record_count: u64,
    stripe_count: u64,
    schema_bytes: u64,
}

#[derive(Serialize)]
struct DataRefInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "ref")]
    url: Option<String>,
    size: u64,
    at: u64,
}

#[derive(Serialize)]
struct SchemaInfo {
    record_field_count: usize,
    total_field_count: usize,
    fields: Vec<SchemaFieldInfo>,
}

#[derive(Serialize)]
struct SchemaFieldInfo {
    name: String,
    schema_id: u32,
    #[serde(rename = "type")]
    basic_type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    children: Vec<SchemaFieldInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptor: Option<FieldDescriptorInfo>,
}

#[derive(Serialize)]
struct UrlListInfo {
    count: usize,
    urls: Vec<String>,
}

#[derive(Serialize)]
struct StripeInfo {
    stripe_idx: usize,
    total_record_count: u64,
    deleted_record_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptor: Option<StripeDescriptorInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_list_ref: Option<DataRefInfo>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<StripeFieldInfo>,
}

#[derive(Serialize)]
struct StripeDescriptorInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    properties_ref: Option<DataRefInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexes_ref: Option<DataRefInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stored_data_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stored_index_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plain_data_size: Option<u64>,
    record_offset: u64,
}

#[derive(Serialize)]
struct StripeFieldInfo {
    name: String,
    schema_id: u32,
    #[serde(rename = "type")]
    basic_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    position_count: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    encodings: Vec<EncodingInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptor: Option<FieldDescriptorInfo>,
}

#[derive(Serialize)]
struct EncodingInfo {
    index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    buffer_count: Option<usize>,
}

#[derive(Serialize)]
struct FieldDescriptorInfo {
    position_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    null_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    constant_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dictionary_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nan_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stored_data_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stored_index_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plain_data_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_stats: Option<RangeStatsInfo>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    properties: Vec<PropertyInfo>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    custom_properties: Vec<PropertyInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    membership_filters: Option<MembershipFiltersInfo>,
}

#[derive(Serialize)]
struct RangeStatsInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    min_value: Option<String>,
    min_inclusive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_value: Option<String>,
    max_inclusive: bool,
}

#[derive(Serialize)]
struct PropertyInfo {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

#[derive(Serialize)]
struct MembershipFiltersInfo {
    bloom_filter_count: usize,
}

/// Run the inspect command
pub fn run(verbose: u8, shard_path: String) -> Result<()> {
    let shard_path = file_path_to_object_url(&shard_path)?;
    println!("Inspecting shard: {}", shard_path.as_str());

    let object_store = get_object_store(&shard_path)?;
    let shard = ShardOptions::new(object_store)
        .open(shard_path)
        .context("Failed to open shard")?;

    // Shard directory
    let shard_directory = create_shard_directory_info(shard.directory())?;

    // Schema
    let schema = shard
        .fetch_schema()
        .context("Failed to fetch shard schema")?;
    let schema_info = create_schema_info(schema, &shard, verbose)?;

    // URL list
    let url_list = shard.fetch_url_list().context("Failed to fetch URL list")?;
    let url_list_info = create_url_list_info(url_list);

    // Stripes
    let stripe_count = shard.stripe_count();
    let mut stripes_info = Vec::new();

    for stripe_index in 0..stripe_count {
        let stripe = shard
            .open_stripe(stripe_index)
            .with_context(|| format!("Failed to open stripe {}", stripe_index))?;
        let stripe_info = create_stripe_info(&shard, &stripe, stripe_index, verbose)?;
        stripes_info.push(stripe_info);
    }

    let summary = InspectSummary {
        shard_directory,
        schema: schema_info,
        url_list: url_list_info,
        stripes: stripes_info,
    };

    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn get_object_store(shard_url: &ObjectUrl) -> Result<Arc<dyn ObjectStore>> {
    if shard_url.as_str().starts_with("file://") {
        Ok(Arc::new(LocalFsObjectStore::new_unscoped()))
    } else {
        anyhow::bail!(
            "Unsupported URL scheme '{}'. Only 'file://' URLs are supported currently.",
            shard_url.as_str().split(':').next().unwrap_or("unknown")
        );
    }
}

fn create_shard_directory_info(shard_dir: &ShardDirectory) -> Result<ShardDirectoryInfo> {
    Ok(ShardDirectoryInfo {
        total_record_count: shard_dir.total_record_count,
        deleted_record_count: shard_dir.deleted_record_count,
        stripe_count: shard_dir.stripe_count,
        schema_bytes: shard_dir.schema_ref.as_ref().map(|d| d.len()).unwrap_or(0),
    })
}

fn create_data_ref_info(data_ref: &DataRef) -> DataRefInfo {
    DataRefInfo {
        url: if data_ref.url.is_empty() {
            None
        } else {
            Some(data_ref.url.clone())
        },
        size: data_ref.len(),
        at: data_ref.range.map(|r| r.start).unwrap_or(0),
    }
}

/// Creates a `FieldDescriptorInfo` from a shard-level field descriptor.
///
/// This function extracts the key properties from a field descriptor and formats them
/// for display in the inspect output.
fn create_field_descriptor_info(
    field_desc: &amudai_format::defs::shard::FieldDescriptor,
) -> FieldDescriptorInfo {
    FieldDescriptorInfo {
        position_count: field_desc.position_count,
        null_count: field_desc.null_count,
        constant_value: field_desc
            .constant_value
            .as_ref()
            .map(|v| format!("{:?}", v)),
        dictionary_size: field_desc.dictionary_size,
        nan_count: field_desc.nan_count,
        stored_data_size: field_desc.stored_data_size,
        stored_index_size: field_desc.stored_index_size,
        plain_data_size: field_desc.plain_data_size,
        range_stats: field_desc.range_stats.as_ref().map(create_range_stats_info),
        properties: field_desc
            .properties
            .iter()
            .map(create_property_info)
            .collect(),
        custom_properties: field_desc
            .custom_properties
            .iter()
            .map(create_property_info)
            .collect(),
        membership_filters: field_desc
            .membership_filters
            .as_ref()
            .map(create_membership_filters_info),
    }
}

/// Creates a `FieldDescriptorInfo` from a stripe-level field descriptor.
///
/// This function extracts the key properties from a stripe field descriptor and formats them
/// for display in the inspect output. It handles the stripe-specific structure where the
/// field descriptor is wrapped in a StripeFieldDescriptor.
fn create_stripe_field_descriptor_info(
    stripe_field_desc: &amudai_shard::read::stripe::StripeFieldDescriptor,
) -> Option<FieldDescriptorInfo> {    stripe_field_desc.field.as_ref().map(|field_desc| {
        FieldDescriptorInfo {
            position_count: field_desc.position_count,
            null_count: field_desc.null_count,
            constant_value: field_desc
                .constant_value
                .as_ref()
                .map(|v| format!("{:?}", v)),
            dictionary_size: field_desc.dictionary_size,
            nan_count: field_desc.nan_count,
            stored_data_size: field_desc.stored_data_size,
            stored_index_size: field_desc.stored_index_size,
            plain_data_size: field_desc.plain_data_size,
            range_stats: field_desc.range_stats.as_ref().map(create_range_stats_info),
            properties: field_desc
                .properties
                .iter()
                .map(create_property_info)
                .collect(),
            custom_properties: field_desc
                .custom_properties
                .iter()
                .map(create_property_info)
                .collect(),
            membership_filters: field_desc
                .membership_filters
                .as_ref()
                .map(create_membership_filters_info),
        }
    })
}

/// Creates a `RangeStatsInfo` from a `RangeStats`.
fn create_range_stats_info(range_stats: &amudai_format::defs::shard::RangeStats) -> RangeStatsInfo {
    RangeStatsInfo {
        min_value: range_stats.min_value.as_ref().map(|v| format!("{:?}", v)),
        min_inclusive: range_stats.min_inclusive,
        max_value: range_stats.max_value.as_ref().map(|v| format!("{:?}", v)),
        max_inclusive: range_stats.max_inclusive,
    }
}

/// Creates a `PropertyInfo` from a `NameValuePair`.
fn create_property_info(nvp: &amudai_format::defs::common::NameValuePair) -> PropertyInfo {
    PropertyInfo {
        name: nvp.name.clone(),
        value: nvp.value.as_ref().map(|v| format!("{:?}", v)),
    }
}

/// Creates a `MembershipFiltersInfo` from a `MembershipFilters`.
fn create_membership_filters_info(
    filters: &amudai_format::defs::shard::MembershipFilters,
) -> MembershipFiltersInfo {
    MembershipFiltersInfo {
        bloom_filter_count: filters.bloom_filters.len(),
    }
}

/// Recursively traverses a DataType tree and collects SchemaFieldInfo for each nested child node.
fn traverse_schema_data_type_tree(
    data_type: &amudai_format::schema::DataType,
    shard: &amudai_shard::read::shard::Shard,
    children: &mut Vec<SchemaFieldInfo>,
    verbose: u8,
) -> Result<()> {
    let child_count = data_type.child_count()?;
    for i in 0..child_count {
        let child_data_type = data_type.child_at(i)?;
        let schema_id = child_data_type.schema_id()?;

        // Get shard-level field descriptor if verbosity >= 1
        let shard_descriptor = if verbose >= 1 {
            shard
                .fetch_field_descriptor(schema_id)
                .ok()
                .map(|desc| create_field_descriptor_info(&desc))
        } else {
            None
        };

        let mut child_info = SchemaFieldInfo {
            name: child_data_type.name()?.to_string(),
            schema_id: schema_id.as_u32(),
            basic_type: format_basic_type_compact(&child_data_type.describe()?),
            children: Vec::new(),
            descriptor: shard_descriptor,
        };

        // Recursively traverse nested children
        traverse_schema_data_type_tree(&child_data_type, shard, &mut child_info.children, verbose)?;

        children.push(child_info);
    }
    Ok(())
}

/// Recursively counts all DataType nodes in a DataType tree (including the root node).
fn count_data_type_nodes(data_type: &amudai_format::schema::DataType) -> Result<usize> {
    let mut count = 1; // Count the current node
    let child_count = data_type.child_count()?;
    for i in 0..child_count {
        let child_data_type = data_type.child_at(i)?;
        count += count_data_type_nodes(&child_data_type)?;
    }
    Ok(count)
}

fn create_schema_info(
    schema: &amudai_format::schema::Schema,
    shard: &amudai_shard::read::shard::Shard,
    verbose: u8,
) -> Result<SchemaInfo> {
    let field_list = schema.field_list()?;
    let mut fields = Vec::new();
    let mut total_field_count = 0;

    for i in 0..field_list.len()? {
        let field = field_list.get_at(i)?;
        let schema_id = field.schema_id()?;

        // Get shard-level field descriptor if verbosity >= 1
        let shard_descriptor = if verbose >= 1 {
            shard
                .fetch_field_descriptor(schema_id)
                .ok()
                .map(|desc| create_field_descriptor_info(&desc))
        } else {
            None
        };

        let mut field_info = SchemaFieldInfo {
            name: field.name()?.to_string(),
            schema_id: schema_id.as_u32(),
            basic_type: format_basic_type_compact(&field.describe()?),
            children: Vec::new(),
            descriptor: shard_descriptor,
        };

        // Count all DataType nodes (including this field and all its children)
        total_field_count += count_data_type_nodes(&field)?;

        // Recursively traverse child DataTypes
        traverse_schema_data_type_tree(&field, shard, &mut field_info.children, verbose)?;

        fields.push(field_info);
    }

    Ok(SchemaInfo {
        record_field_count: schema.len()?,
        total_field_count,
        fields,
    })
}

fn create_url_list_info(url_list: &amudai_format::defs::shard::UrlList) -> UrlListInfo {
    UrlListInfo {
        count: url_list.urls.len(),
        urls: url_list.urls.clone(),
    }
}

/// Formats a BasicTypeDescriptor as a compact string representation.
///
/// Examples:
/// - Int64 with signed=true -> "Int64"
/// - Int32 with signed=false -> "UInt32"
/// - FixedSizeList with fixed_size=15 -> "FixedSizeList(15)"
/// - FixedSizeBinary with fixed_size=16 -> "FixedSizeBinary(16)"
fn format_basic_type_compact(desc: &amudai_format::schema::BasicTypeDescriptor) -> String {
    use amudai_format::schema::BasicType;

    match desc.basic_type {
        BasicType::Unit => "Unit".to_string(),
        BasicType::Boolean => "Boolean".to_string(),
        BasicType::Int8 => {
            if desc.signed {
                "Int8".to_string()
            } else {
                "UInt8".to_string()
            }
        }
        BasicType::Int16 => {
            if desc.signed {
                "Int16".to_string()
            } else {
                "UInt16".to_string()
            }
        }
        BasicType::Int32 => {
            if desc.signed {
                "Int32".to_string()
            } else {
                "UInt32".to_string()
            }
        }
        BasicType::Int64 => {
            if desc.signed {
                "Int64".to_string()
            } else {
                "UInt64".to_string()
            }
        }
        BasicType::Float32 => "Float32".to_string(),
        BasicType::Float64 => "Float64".to_string(),
        BasicType::Binary => "Binary".to_string(),
        BasicType::FixedSizeBinary => format!("FixedSizeBinary({})", desc.fixed_size),
        BasicType::String => "String".to_string(),
        BasicType::Guid => "Guid".to_string(),
        BasicType::DateTime => "DateTime".to_string(),
        BasicType::List => "List".to_string(),
        BasicType::FixedSizeList => format!("FixedSizeList({})", desc.fixed_size),
        BasicType::Struct => "Struct".to_string(),
        BasicType::Map => "Map".to_string(),
        BasicType::Union => "Union".to_string(),
    }
}

fn create_stripe_info(
    shard: &amudai_shard::read::shard::Shard,
    stripe: &amudai_shard::read::stripe::Stripe,
    stripe_index: usize,
    verbose: u8,
) -> Result<StripeInfo> {
    let directory = stripe.directory();
    let mut fields = Vec::new();

    if verbose > 0 {
        // Get schema to iterate over fields
        let schema = shard.fetch_schema()?;
        let field_list = schema.field_list()?;
        for i in 0..field_list.len()? {
            let field = field_list.get_at(i)?;

            // Recursively traverse all DataTypes (top-level field and all nested fields)
            traverse_data_type_tree(&field, stripe, &mut fields, verbose)?;
        }
    }

    // Create stripe descriptor info if verbosity >= 1
    let descriptor = if verbose >= 1 {
        Some(StripeDescriptorInfo {
            properties_ref: directory.properties_ref.as_ref().map(create_data_ref_info),
            indexes_ref: directory.indexes_ref.as_ref().map(create_data_ref_info),
            stored_data_size: directory.stored_data_size,
            stored_index_size: directory.stored_index_size,
            plain_data_size: directory.plain_data_size,
            record_offset: directory.record_offset,
        })
    } else {
        None
    };

    Ok(StripeInfo {
        stripe_idx: stripe_index,
        total_record_count: directory.total_record_count,
        deleted_record_count: directory.deleted_record_count,
        descriptor,
        field_list_ref: directory.field_list_ref.as_ref().map(create_data_ref_info),
        fields,
    })
}

/// Recursively traverses a DataType tree and collects StripeFieldInfo for each node.
fn traverse_data_type_tree(
    data_type: &amudai_format::schema::DataType,
    stripe: &amudai_shard::read::stripe::Stripe,
    fields: &mut Vec<StripeFieldInfo>,
    verbose: u8,
) -> Result<()> {
    let schema_id = data_type.schema_id()?;

    // Try to get field descriptor for this DataType node
    if let Ok(field_desc) = stripe.fetch_field_descriptor(schema_id) {
        let mut encodings = Vec::new();

        if verbose > 1 {
            for (i, encoding) in field_desc.encodings.iter().enumerate() {
                let (kind, buffer_count) = if let Some(kind) = &encoding.kind {
                    match kind {
                        amudai_format::defs::shard::data_encoding::Kind::Native(native) => {
                            (Some("Native".to_string()), Some(native.buffers.len()))
                        }
                        amudai_format::defs::shard::data_encoding::Kind::Parquet(_) => {
                            (Some("Parquet".to_string()), None)
                        }
                    }
                } else {
                    (None, None)
                };

                encodings.push(EncodingInfo {
                    index: i,
                    kind,
                    buffer_count,
                });
            }
        }
        fields.push(StripeFieldInfo {
            name: data_type.name()?.to_string(),
            schema_id: schema_id.as_u32(),
            basic_type: format_basic_type_compact(&data_type.describe()?),
            position_count: field_desc.field.as_ref().map(|f| f.position_count),
            encodings,
            descriptor: if verbose >= 2 {
                create_stripe_field_descriptor_info(&field_desc)
            } else {
                None
            },
        });
    }

    // Recursively traverse child DataTypes
    let child_count = data_type.child_count()?;
    for i in 0..child_count {
        let child_data_type = data_type.child_at(i)?;
        traverse_data_type_tree(&child_data_type, stripe, fields, verbose)?;
    }

    Ok(())
}
