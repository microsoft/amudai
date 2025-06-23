//! Utilities for working with shard field descriptors.
//!
//! This module provides functionality for populating and manipulating
//! `shard::FieldDescriptor` instances, including propagating statistics
//! from encoded field data and merging statistics across stripes.

use crate::write::field_encoder::{EncodedField, EncodedFieldStatistics};
use amudai_common::Result;
use amudai_format::defs::{anyvalue_ext::compare_any_values, shard};

/// Creates a new field descriptor with the specified position count and
/// populates it with statistics from the encoded field if available.
///
/// # Arguments
///
/// * `position_count` - The number of logical positions in this field
/// * `encoded_field` - The encoded field containing optional statistics
///
/// # Returns
///
/// A `shard::FieldDescriptor` with statistics populated from the encoded field
pub fn create_with_stats(
    position_count: u64,
    encoded_field: &EncodedField,
) -> shard::FieldDescriptor {
    let mut descriptor = shard::FieldDescriptor {
        position_count,
        ..Default::default()
    };

    populate_statistics(&mut descriptor, encoded_field);
    descriptor
}

/// Populates an existing field descriptor with statistics from the encoded field.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate with statistics
/// * `encoded_field` - The encoded field containing optional statistics
pub fn populate_statistics(descriptor: &mut shard::FieldDescriptor, encoded_field: &EncodedField) {
    if let Some(ref stats) = encoded_field.statistics {
        match stats {
            EncodedFieldStatistics::Primitive(primitive_stats) => {
                populate_primitive_statistics(descriptor, primitive_stats);
            }
            EncodedFieldStatistics::String(string_stats) => {
                populate_string_statistics(descriptor, string_stats);
            }
            EncodedFieldStatistics::Boolean(boolean_stats) => {
                populate_boolean_statistics(descriptor, boolean_stats);
            }
            EncodedFieldStatistics::Binary(binary_stats) => {
                populate_binary_statistics(descriptor, binary_stats);
            }
        }
    }
}

/// Merges statistics from one field descriptor into another field descriptor.
///
/// This function aggregates statistics from two field descriptors by combining their
/// values appropriately. It handles:
/// - Position count aggregation
/// - Null count aggregation (None if either is None)
/// - NaN count aggregation (None if either is None, for floating-point types)
/// - Range statistics merging (min/max values)
/// - Type-specific statistics merging (e.g., string statistics)
///
/// # Arguments
///
/// * `accumulated` - The field descriptor that accumulates statistics (modified in-place)
/// * `current` - The field descriptor to merge into the accumulated result
///
/// # Returns
///
/// A `Result` indicating success or failure of the merge operation
pub fn merge(
    accumulated: &mut shard::FieldDescriptor,
    current: &shard::FieldDescriptor,
) -> Result<()> {
    // Assert: If accumulated has data, current should also have data
    // This ensures we don't mix non-empty accumulated state with empty current descriptors
    assert!(
        accumulated.position_count == 0 || current.position_count > 0,
        "If accumulated.position_count > 0, then current.position_count should be > 0. \
             accumulated.position_count: {}, current.position_count: {}",
        accumulated.position_count,
        current.position_count
    );

    // Early return if current descriptor has no data to contribute
    if current.position_count == 0 {
        return Ok(());
    }

    // If accumulated is in initial state (empty), just copy current into it
    if accumulated.position_count == 0 {
        *accumulated = current.clone();
        return Ok(());
    }

    // Normal merging logic for non-initial state
    accumulated.position_count += current.position_count;

    // Merge null count statistics
    // If any descriptor has None for null_count, the result should be None
    match (accumulated.null_count, current.null_count) {
        (Some(accumulated_null_count), Some(current_null_count)) => {
            accumulated.null_count = Some(accumulated_null_count + current_null_count);
        }
        (_, _) => {
            // If either is None, result is None
            accumulated.null_count = None;
        }
    }

    // Merge NaN count statistics (only applicable for floating-point types)
    // NaN count is only set for floating-point types (f32/f64) during statistics population.
    // For non-floating-point types, nan_count is None.
    //
    // The merging logic follows these principles:
    // 1. If both descriptors have Some(count), sum them (both are floating-point)
    // 2. For any other case (None/None, Some/None, None/Some), set to None
    match (accumulated.nan_count, current.nan_count) {
        (Some(accumulated_nan_count), Some(current_nan_count)) => {
            // Both descriptors represent floating-point fields, sum the NaN counts
            accumulated.nan_count = Some(accumulated_nan_count + current_nan_count);
        }
        _ => {
            // All other cases: set to None
            // - (None, None): Both are non-floating-point fields, keep None
            // - Mixed types: Type inconsistency, handle gracefully by setting to None
            accumulated.nan_count = None;
        }
    }

    // Merge range statistics (min/max values) with sticky None behavior
    //
    // With the early return for current.position_count == 0, we know that current has data.
    // The range statistics merging implements these rules:
    // 1. If both have Some, merge them normally
    // 2. If current has None, disable range stats (sticky None)
    // 3. If accumulated has None and current has Some, this is a sticky None situation - keep it None

    match (&mut accumulated.range_stats, &current.range_stats) {
        (Some(accumulated_range), Some(current_range)) => {
            // Both have range stats - merge them normally
            merge_range_stats(accumulated_range, current_range)?;
        }
        (None, Some(_)) => {
            // Accumulated is None, current has Some
            // This is a sticky None situation - keep it None
        }
        (_, None) => {
            // Current has no range stats - disable them (makes them sticky)
            accumulated.range_stats = None;
        }
    }

    // Merge type-specific statistics (e.g., string statistics)
    if let Some(ref current_type_specific) = current.type_specific {
        match &mut accumulated.type_specific {
            Some(accumulated_type_specific) => {
                // Merge existing type-specific statistics
                merge_type_specific_stats(accumulated_type_specific, current_type_specific)?;
            }
            None => {
                // Accumulated has None, this maintains consistency with null_count and nan_count behavior
                // Keep type_specific as None
            }
        }
    }

    Ok(())
}

/// Populates field descriptor with primitive statistics.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate
/// * `primitive_stats` - The primitive statistics to copy from
fn populate_primitive_statistics(
    descriptor: &mut shard::FieldDescriptor,
    primitive_stats: &amudai_data_stats::primitive::PrimitiveStats,
) {
    // Map null count
    descriptor.null_count = Some(primitive_stats.null_count as u64);

    // Map NaN count based on data type:
    // - For floating-point types (f32/f64): always set nan_count (even if 0)
    // - For non-floating-point types: set to None since NaN doesn't apply
    descriptor.nan_count = match primitive_stats.basic_type.basic_type {
        amudai_format::schema::BasicType::Float32 | amudai_format::schema::BasicType::Float64 => {
            Some(primitive_stats.nan_count as u64)
        }
        _ => None,
    };

    // Map range statistics (always set for primitive types, even if min/max are None)
    // This distinguishes between:
    // - None: Range statistics were not collected (not applicable for this type)
    // - Some(RangeStats { min_value: None, max_value: None }): Range stats collected but no values found
    descriptor.range_stats = Some(primitive_stats.range_stats.clone());
}

/// Populates field descriptor with string statistics.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate
/// * `string_stats` - The string statistics to copy from
fn populate_string_statistics(
    descriptor: &mut shard::FieldDescriptor,
    string_stats: &amudai_data_stats::string::StringStats,
) {
    // Map null count
    descriptor.null_count = Some(string_stats.null_count as u64);

    // Set string-specific statistics in type_specific field
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::StringStats(
        shard::StringStats {
            min_size: string_stats.min_size,
            min_non_empty_size: string_stats.min_non_empty_size,
            max_size: string_stats.max_size,
            ascii_count: Some(string_stats.ascii_count),
        },
    ));
}

/// Populates field descriptor with boolean statistics.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate
/// * `boolean_stats` - The boolean statistics to copy from
fn populate_boolean_statistics(
    descriptor: &mut shard::FieldDescriptor,
    boolean_stats: &amudai_data_stats::boolean::BooleanStats,
) {
    // Map null count
    descriptor.null_count = Some(boolean_stats.null_count as u64);

    // Set boolean-specific statistics in type_specific field
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::BooleanStats(
        shard::BooleanStats {
            true_count: boolean_stats.true_count as u64,
            false_count: boolean_stats.false_count as u64,
        },
    ));
}

/// Populates field descriptor with binary statistics.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate
/// * `binary_stats` - The binary statistics to copy from
fn populate_binary_statistics(
    descriptor: &mut shard::FieldDescriptor,
    binary_stats: &amudai_data_stats::binary::BinaryStats,
) {
    // Map null count
    descriptor.null_count = Some(binary_stats.null_count);

    // Set binary-specific statistics in type_specific field using ContainerStats
    // which is appropriate for binary data (similar to how it's used for lists/maps)
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::ContainerStats(
        shard::ContainerStats {
            min_length: binary_stats.min_length,
            max_length: binary_stats.max_length,
            min_non_empty_length: binary_stats.min_non_empty_length,
        },
    ));
}

/// Merges range statistics by updating min and max values appropriately.
///
/// This function compares minimum and maximum values from field descriptor statistics
/// with existing accumulated statistics and updates the accumulated values to maintain
/// the overall min/max across all processed descriptors.
///
/// # Arguments
///
/// * `accumulated_range` - The accumulated range statistics to update
/// * `current_range` - The current range statistics to merge from
///
/// # Returns
///
/// A `Result` indicating success or failure of the range merge operation
fn merge_range_stats(
    accumulated_range: &mut shard::RangeStats,
    current_range: &shard::RangeStats,
) -> Result<()> {
    // Merge minimum values
    match (&accumulated_range.min_value, &current_range.min_value) {
        (Some(accumulated_min), Some(current_min)) => {
            // Compare and keep the smaller value
            if compare_any_values(current_min, accumulated_min)? < 0 {
                accumulated_range.min_value = Some(current_min.clone());
                accumulated_range.min_inclusive = current_range.min_inclusive;
            }
        }
        (None, Some(_)) => {
            accumulated_range.min_value = current_range.min_value.clone();
            accumulated_range.min_inclusive = current_range.min_inclusive;
        }
        _ => {
            // Either both None or accumulated has value but stripe doesn't - no change needed
        }
    }

    // Merge maximum values
    match (&accumulated_range.max_value, &current_range.max_value) {
        (Some(accumulated_max), Some(current_max)) => {
            // Compare and keep the larger value
            if compare_any_values(current_max, accumulated_max)? > 0 {
                accumulated_range.max_value = Some(current_max.clone());
                accumulated_range.max_inclusive = current_range.max_inclusive;
            }
        }
        (None, Some(_)) => {
            accumulated_range.max_value = current_range.max_value.clone();
            accumulated_range.max_inclusive = current_range.max_inclusive;
        }
        _ => {
            // Either both None or accumulated has value but stripe doesn't - no change needed
        }
    }

    Ok(())
}

/// Merges type-specific statistics from stripe-level to accumulated level.
///
/// This function handles merging of type-specific statistics such as string statistics.
/// Different types require different merging logic.
///
/// # Arguments
///
/// * `accumulated` - The accumulated type-specific statistics to update
/// * `current` - The current stripe's type-specific statistics to merge from
///
/// # Returns
///
/// A `Result` indicating success or failure of the merge operation
fn merge_type_specific_stats(
    accumulated: &mut shard::field_descriptor::TypeSpecific,
    current: &shard::field_descriptor::TypeSpecific,
) -> Result<()> {
    use shard::field_descriptor::TypeSpecific;
    match (accumulated, current) {
        (
            TypeSpecific::StringStats(accumulated_stats),
            TypeSpecific::StringStats(current_stats),
        ) => {
            // Merge string statistics by taking min of mins, max of maxes, and sum of ascii_count
            accumulated_stats.min_size = accumulated_stats.min_size.min(current_stats.min_size);
            accumulated_stats.max_size = accumulated_stats.max_size.max(current_stats.max_size);

            // For min_non_empty_size, take the minimum of the non-None values
            match (
                &accumulated_stats.min_non_empty_size,
                &current_stats.min_non_empty_size,
            ) {
                (Some(accumulated_min_non_empty), Some(current_min_non_empty)) => {
                    accumulated_stats.min_non_empty_size =
                        Some((*accumulated_min_non_empty).min(*current_min_non_empty));
                }
                (None, Some(current_min_non_empty)) => {
                    accumulated_stats.min_non_empty_size = Some(*current_min_non_empty);
                }
                // If accumulated has value but stripe doesn't, or both are None, keep accumulated value
                _ => {}
            }

            // Sum ascii_count if both are present, otherwise result is None
            match (&accumulated_stats.ascii_count, &current_stats.ascii_count) {
                (Some(accumulated_ascii), Some(current_ascii)) => {
                    accumulated_stats.ascii_count = Some(accumulated_ascii + current_ascii);
                }
                (_, _) => {
                    // If either is None, result is None
                    accumulated_stats.ascii_count = None;
                }
            }
        }
        (
            TypeSpecific::BooleanStats(accumulated_stats),
            TypeSpecific::BooleanStats(current_stats),
        ) => {
            // Merge boolean statistics by summing true and false counts
            accumulated_stats.true_count += current_stats.true_count;
            accumulated_stats.false_count += current_stats.false_count;
        }
        (
            TypeSpecific::ContainerStats(accumulated_stats),
            TypeSpecific::ContainerStats(current_stats),
        ) => {
            // Merge container statistics (for binary, list, and map types)
            // by taking min of mins, max of maxes, and handling min_non_empty_length
            accumulated_stats.min_length =
                accumulated_stats.min_length.min(current_stats.min_length);
            accumulated_stats.max_length =
                accumulated_stats.max_length.max(current_stats.max_length);

            // For min_non_empty_length, take the minimum of the non-None values
            match (
                &accumulated_stats.min_non_empty_length,
                &current_stats.min_non_empty_length,
            ) {
                (Some(accumulated_min_non_empty), Some(current_min_non_empty)) => {
                    accumulated_stats.min_non_empty_length =
                        Some((*accumulated_min_non_empty).min(*current_min_non_empty));
                }
                (None, Some(current_min_non_empty)) => {
                    accumulated_stats.min_non_empty_length = Some(*current_min_non_empty);
                }
                // If accumulated has value but current doesn't, or both are None, keep accumulated value
                _ => {}
            }
        }
        _ => {
            // Mismatched types - this shouldn't happen in a well-formed shard
            // but we'll just keep the accumulated statistics without error
        }
    }

    Ok(())
}

/// Merges a collection of field descriptors into a single consolidated descriptor.
///
/// This function takes a collection of `Option<shard::FieldDescriptor>` and merges
/// them into a single result following these rules:
/// - If any descriptor is None, returns None
/// - If all descriptors are Some, merges them using `merge_stripe_field_descriptor`
/// - If the collection is empty, returns None
///
/// This is useful for consolidating field descriptors from multiple sources (e.g.,
/// multiple stripes or shards) into a single unified view, but only when all
/// sources have valid descriptors.
///
/// # Arguments
///
/// * `descriptors` - An iterator over `Option<shard::FieldDescriptor>` to merge
///
/// # Returns
///
/// * `Ok(Some(descriptor))` - If all descriptors were Some and successfully merged
/// * `Ok(None)` - If any descriptor was None or the collection was empty
/// * `Err(error)` - If merging failed due to incompatible data
///
/// # Example
///
/// ```rust
/// use amudai_shard::write::field_descriptor::merge_field_descriptors;
/// use amudai_format::defs::shard::FieldDescriptor;
///
/// let desc1 = Some(FieldDescriptor { position_count: 100, ..Default::default() });
/// let desc2 = Some(FieldDescriptor { position_count: 200, ..Default::default() });
/// let desc3 = None;
///
/// // This will return None because desc3 is None
/// let merged = merge_field_descriptors(
///     vec![desc1, desc2, desc3].into_iter()
/// ).unwrap();
/// assert!(merged.is_none());
/// ```
pub fn merge_field_descriptors<I>(descriptors: I) -> Result<Option<shard::FieldDescriptor>>
where
    I: IntoIterator<Item = Option<shard::FieldDescriptor>>,
{
    let descriptors: Vec<_> = descriptors.into_iter().collect();

    // If empty collection, return None
    if descriptors.is_empty() {
        return Ok(None);
    }

    // If any descriptor is None, return None
    if descriptors.iter().any(|desc| desc.is_none()) {
        return Ok(None);
    }

    // All descriptors are Some, so we can safely unwrap and merge them
    let mut result: Option<shard::FieldDescriptor> = None;

    for descriptor_opt in descriptors {
        let descriptor = descriptor_opt.unwrap(); // Safe because we checked above
        match result {
            None => {
                // First descriptor - clone it as the starting point
                result = Some(descriptor);
            }
            Some(ref mut accumulated) => {
                // Merge this descriptor into the accumulated result
                merge(accumulated, &descriptor)?;
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_data_stats::primitive::PrimitiveStats;
    use amudai_format::defs::common::{AnyValue, any_value::Kind};
    use amudai_format::defs::schema_ext::BasicTypeDescriptor;
    use amudai_format::defs::shard::RangeStats;

    #[test]
    fn test_create_with_stats_no_statistics() {
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: None,
        };

        let descriptor = create_with_stats(100, &encoded_field);

        assert_eq!(descriptor.position_count, 100);
        assert_eq!(descriptor.null_count, None);
        assert_eq!(descriptor.nan_count, None);
        assert!(descriptor.range_stats.is_none());
    }

    #[test]
    fn test_create_with_stats_primitive_statistics() {
        let primitive_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Float32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 50,
            null_count: 5,
            nan_count: 2,
            range_stats: RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(10)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(100)),
                    annotation: None,
                }),
                max_inclusive: true,
            },
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(primitive_stats)),
        };

        let descriptor = create_with_stats(50, &encoded_field);

        assert_eq!(descriptor.position_count, 50);
        assert_eq!(descriptor.null_count, Some(5));
        assert_eq!(descriptor.nan_count, Some(2));
        assert!(descriptor.range_stats.is_some());

        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_some());
        assert!(range_stats.max_value.is_some());
    }

    #[test]
    fn test_populate_statistics_zero_nan_count() {
        let primitive_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 50,
            null_count: 5,
            nan_count: 0, // Zero NaN count should result in None for non-FP types
            range_stats: RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            },
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(primitive_stats)),
        };

        let mut descriptor = shard::FieldDescriptor {
            position_count: 50,
            ..Default::default()
        };

        populate_statistics(&mut descriptor, &encoded_field);

        assert_eq!(descriptor.null_count, Some(5));
        assert_eq!(descriptor.nan_count, None); // Should be None for non-FP types
        assert!(descriptor.range_stats.is_some()); // Should always be Some for primitive types

        // But the min/max values inside should be None
        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_none());
        assert!(range_stats.max_value.is_none());
    }

    #[test]
    fn test_merge_stripe_field_descriptor_simple() {
        let mut shard_field = shard::FieldDescriptor {
            position_count: 0,
            null_count: None,
            nan_count: None,
            range_stats: None,
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(5),
            nan_count: Some(2),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(10)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        assert_eq!(shard_field.position_count, 50); // 0 + 50
        assert_eq!(shard_field.null_count, Some(5)); // None + Some(5) = Some(5) since accumulated position_count was 0
        assert_eq!(shard_field.nan_count, Some(2)); // None + Some(2) = Some(2) since accumulated position_count was 0
        assert!(shard_field.range_stats.is_some());
    }

    #[test]
    fn test_merge_stripe_field_descriptor_range_stats() {
        let mut shard_field = shard::FieldDescriptor {
            position_count: 100,
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(15)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)), // Lower than existing min (5)
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(20)), // Higher than existing max (15)
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        let range_stats = shard_field.range_stats.unwrap();

        // Min should be updated to the lower value (1)
        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 1);
        } else {
            panic!("Expected I64Value for min");
        }

        // Max should be updated to the higher value (20)
        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 20);
        } else {
            panic!("Expected I64Value for max");
        }
    }

    #[test]
    fn test_create_with_stats_string_statistics() {
        let string_stats = amudai_data_stats::string::StringStats {
            min_size: 0,
            min_non_empty_size: Some(3),
            max_size: 15,
            ascii_count: 8,
            count: 10,
            null_count: 2,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::String(string_stats)),
        };

        let descriptor = create_with_stats(10, &encoded_field);

        assert_eq!(descriptor.position_count, 10);
        assert_eq!(descriptor.null_count, Some(2));

        // Check string-specific statistics
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(proto_string_stats)) =
            &descriptor.type_specific
        {
            assert_eq!(proto_string_stats.min_size, 0);
            assert_eq!(proto_string_stats.min_non_empty_size, Some(3));
            assert_eq!(proto_string_stats.max_size, 15);
            assert_eq!(proto_string_stats.ascii_count, Some(8));
        } else {
            panic!("Expected string statistics in type_specific field");
        }
    }

    #[test]
    fn test_populate_string_statistics() {
        let string_stats = amudai_data_stats::string::StringStats {
            min_size: 1,
            min_non_empty_size: None,
            max_size: 50,
            ascii_count: 100,
            count: 150,
            null_count: 0,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::String(string_stats)),
        };

        let mut descriptor = shard::FieldDescriptor {
            position_count: 150,
            ..Default::default()
        };

        populate_statistics(&mut descriptor, &encoded_field);

        assert_eq!(descriptor.null_count, Some(0));
        assert!(descriptor.type_specific.is_some());

        if let Some(shard::field_descriptor::TypeSpecific::StringStats(proto_string_stats)) =
            &descriptor.type_specific
        {
            assert_eq!(proto_string_stats.min_size, 1);
            assert_eq!(proto_string_stats.min_non_empty_size, None);
            assert_eq!(proto_string_stats.max_size, 50);
            assert_eq!(proto_string_stats.ascii_count, Some(100));
        } else {
            panic!("Expected string statistics in type_specific field");
        }
    }

    #[test]
    fn test_merge_string_statistics() {
        let mut shard_field = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(5),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 2,
                    min_non_empty_size: Some(3),
                    max_size: 10,
                    ascii_count: Some(80),
                },
            )),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(2),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 1,                 // Lower than shard min (2)
                    min_non_empty_size: Some(2), // Lower than shard min_non_empty (3)
                    max_size: 15,                // Higher than shard max (10)
                    ascii_count: Some(40),
                },
            )),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        // Verify aggregated statistics
        assert_eq!(shard_field.position_count, 150); // 100 + 50
        assert_eq!(shard_field.null_count, Some(7)); // 5 + 2

        // Verify merged string statistics
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(merged_stats)) =
            &shard_field.type_specific
        {
            assert_eq!(merged_stats.min_size, 1); // min(2, 1)
            assert_eq!(merged_stats.min_non_empty_size, Some(2)); // min(3, 2)
            assert_eq!(merged_stats.max_size, 15); // max(10, 15)
            assert_eq!(merged_stats.ascii_count, Some(120)); // 80 + 40
        } else {
            panic!("Expected string statistics in type_specific field");
        }
    }

    #[test]
    fn test_nan_count_behavior_by_data_type() {
        // Test floating-point types - should always set nan_count even when 0
        let float32_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Float32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 10,
            null_count: 0,
            nan_count: 0, // Zero NaNs for float32
            range_stats: RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            },
        };

        let float64_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Float64,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 10,
            null_count: 0,
            nan_count: 3, // Some NaNs for float64
            range_stats: RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            },
        };

        // Test integer types - should never set nan_count
        let int32_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 10,
            null_count: 0,
            nan_count: 0, // Not applicable for integers
            range_stats: RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            },
        };

        // Test Float32 with zero NaNs - should set to Some(0)
        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(float32_stats)),
        };
        populate_statistics(&mut descriptor, &encoded_field);
        assert_eq!(descriptor.nan_count, Some(0)); // Should be Some(0) for FP types

        // Test Float64 with some NaNs - should set to Some(3)
        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(float64_stats)),
        };
        populate_statistics(&mut descriptor, &encoded_field);
        assert_eq!(descriptor.nan_count, Some(3)); // Should be Some(3) for FP types

        // Test Int32 - should never set nan_count regardless of value
        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(int32_stats)),
        };
        populate_statistics(&mut descriptor, &encoded_field);
        assert_eq!(descriptor.nan_count, None); // Should be None for non-FP types
    }

    #[test]
    fn test_range_stats_always_set_for_primitive_types() {
        // Test that range_stats is always set for primitive types, even when min/max are None
        let primitive_stats_empty_range = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int64,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 10,
            null_count: 10, // All nulls
            nan_count: 0,
            range_stats: RangeStats {
                min_value: None, // No min value (all nulls)
                min_inclusive: false,
                max_value: None, // No max value (all nulls)
                max_inclusive: false,
            },
        };

        let primitive_stats_with_range = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int64,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 10,
            null_count: 2,
            nan_count: 0,
            range_stats: RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(15)),
                    annotation: None,
                }),
                max_inclusive: true,
            },
        };

        // Test case 1: All nulls - range_stats should be Some but min/max should be None
        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(
                primitive_stats_empty_range,
            )),
        };
        populate_statistics(&mut descriptor, &encoded_field);

        assert!(descriptor.range_stats.is_some()); // Always Some for primitive types
        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_none()); // But min is None
        assert!(range_stats.max_value.is_none()); // And max is None

        // Test case 2: With actual values - range_stats should be Some with actual min/max
        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(
                primitive_stats_with_range,
            )),
        };
        populate_statistics(&mut descriptor, &encoded_field);

        assert!(descriptor.range_stats.is_some()); // Always Some for primitive types
        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_some()); // Min is Some
        assert!(range_stats.max_value.is_some()); // Max is Some

        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 5);
        } else {
            panic!("Expected I64Value for min");
        }

        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 15);
        } else {
            panic!("Expected I64Value for max");
        }
    }

    #[test]
    fn test_merge_field_descriptors() {
        // Test with all Some descriptors - should succeed
        let desc1 = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
            nan_count: None,
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(25)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        let desc2 = Some(shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(20),
            nan_count: Some(5),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(50)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        // Test with all Some descriptors
        let merged = merge_field_descriptors(vec![desc1.clone(), desc2.clone()]).unwrap();

        assert!(merged.is_some());

        let descriptor = merged.unwrap();
        assert_eq!(descriptor.position_count, 300); // 100 + 200
        assert_eq!(descriptor.null_count, Some(30)); // 10 + 20

        // NaN count should be None because desc1 has None
        assert_eq!(descriptor.nan_count, None);

        // Range stats should be merged - should have the overall min/max
        assert!(descriptor.range_stats.is_some());
        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_some());
        assert!(range_stats.max_value.is_some());

        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 1); // min(5, 1) = 1
        } else {
            panic!("Expected I64Value for min");
        }

        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 50); // max(25, 50) = 50
        } else {
            panic!("Expected I64Value for max");
        }

        // Test with one None descriptor - should return None
        let desc3 = None;
        let merged_with_none = merge_field_descriptors(vec![desc1, desc2, desc3]).unwrap();
        assert!(merged_with_none.is_none());
    }

    #[test]
    fn test_merge_field_descriptors_all_none() {
        let descriptors = vec![None, None, None];
        let merged = merge_field_descriptors(descriptors).unwrap();
        assert!(merged.is_none());
    }

    #[test]
    fn test_merge_field_descriptors_single_some() {
        // Test with only one Some descriptor and no None values
        let desc = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
            nan_count: Some(5),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(100)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        let descriptors = vec![desc];
        let merged = merge_field_descriptors(descriptors).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();
        assert_eq!(result.position_count, 100);
        assert_eq!(result.null_count, Some(10));
        assert_eq!(result.nan_count, Some(5));

        // Verify range_stats are preserved
        assert!(result.range_stats.is_some());
        let range_stats = result.range_stats.unwrap();
        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 1);
        } else {
            panic!("Expected I64Value for min");
        }
        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 100);
        } else {
            panic!("Expected I64Value for max");
        }

        // Test with one Some descriptor mixed with None values - should return None
        let desc = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
            nan_count: Some(5),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(100)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        let descriptors_with_none = vec![None, desc, None];
        let merged_with_none = merge_field_descriptors(descriptors_with_none).unwrap();
        assert!(merged_with_none.is_none());
    }

    #[test]
    fn test_merge_field_descriptors_empty_collection() {
        let descriptors: Vec<Option<shard::FieldDescriptor>> = vec![];
        let merged = merge_field_descriptors(descriptors).unwrap();
        assert!(merged.is_none());
    }

    #[test]
    fn test_merge_field_descriptors_with_string_stats() {
        let desc1 = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 5,
                    min_non_empty_size: Some(5),
                    max_size: 20,
                    ascii_count: Some(80),
                },
            )),
            ..Default::default()
        });

        let desc2 = Some(shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(15),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 3,
                    min_non_empty_size: Some(3),
                    max_size: 30,
                    ascii_count: Some(150),
                },
            )),
            ..Default::default()
        });

        // Test merging with all Some descriptors
        let merged = merge_field_descriptors(vec![desc1.clone(), desc2.clone()]).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();
        assert_eq!(result.position_count, 300);
        assert_eq!(result.null_count, Some(25));

        if let Some(shard::field_descriptor::TypeSpecific::StringStats(string_stats)) =
            &result.type_specific
        {
            assert_eq!(string_stats.min_size, 3); // min of 5 and 3
            assert_eq!(string_stats.min_non_empty_size, Some(3)); // min of 5 and 3
            assert_eq!(string_stats.max_size, 30); // max of 20 and 30
            assert_eq!(string_stats.ascii_count, Some(230)); // sum of 80 and 150
        } else {
            panic!("Expected StringStats");
        }

        // Test with None descriptor mixed in - should return None
        let merged_with_none = merge_field_descriptors(vec![desc1, None, desc2]).unwrap();
        assert!(merged_with_none.is_none());
    }

    #[test]
    fn test_merge_default_with_real_descriptor() {
        // Test the scenario from prepare_field_list where we start with a default descriptor
        let default_desc = shard::FieldDescriptor::default();
        let real_desc = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(50)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        println!(
            "Default descriptor: position_count={}, null_count={:?}",
            default_desc.position_count, default_desc.null_count
        );
        println!(
            "Real descriptor: position_count={}, null_count={:?}",
            real_desc.position_count, real_desc.null_count
        );

        // This simulates what happens in prepare_field_list on the first iteration
        let merged = merge_field_descriptors(vec![Some(default_desc), Some(real_desc)]).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();
        println!(
            "Merged result: position_count={}, null_count={:?}",
            result.position_count, result.null_count
        );

        // The default descriptor has position_count=0, so merging should allow setting null_count
        assert_eq!(result.position_count, 100); // 0 + 100
        assert_eq!(result.null_count, Some(10)); // None + Some(10) = Some(10) since default_desc position_count was 0
    }

    #[test]
    fn test_merge_field_descriptors_with_missing_min_max_values() {
        // Test scenario where some descriptors have missing min/max values
        // and others have actual values - should merge correctly

        // Descriptor 1: Has range stats but min/max are None (e.g., all nulls)
        let desc1 = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(100), // All nulls
            range_stats: Some(RangeStats {
                min_value: None, // No min value
                min_inclusive: false,
                max_value: None, // No max value
                max_inclusive: false,
            }),
            ..Default::default()
        });

        // Descriptor 2: Has actual min/max values
        let desc2 = Some(shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(50),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(15)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        // Descriptor 3: Has different min/max values
        let desc3 = Some(shard::FieldDescriptor {
            position_count: 150,
            null_count: Some(25),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)), // Lower than desc2
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(20)), // Higher than desc2
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        let merged = merge_field_descriptors(vec![desc1, desc2, desc3]).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();

        // Verify aggregated counts
        assert_eq!(result.position_count, 450); // 100 + 200 + 150
        assert_eq!(result.null_count, Some(175)); // 100 + 50 + 25

        // Verify range stats: should have the overall min/max from non-None values
        assert!(result.range_stats.is_some());
        let range_stats = result.range_stats.unwrap();

        // Min should be 1 (from desc3, ignoring desc1's None)
        assert!(range_stats.min_value.is_some());
        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 1);
        } else {
            panic!("Expected I64Value for min");
        }

        // Max should be 20 (from desc3, ignoring desc1's None)
        assert!(range_stats.max_value.is_some());
        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 20);
        } else {
            panic!("Expected I64Value for max");
        }
    }

    #[test]
    fn test_merge_field_descriptors_all_missing_min_max() {
        // Test scenario where all descriptors have missing min/max values
        // The result should still have range_stats but with None min/max

        let desc1 = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(100),
            range_stats: Some(RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            }),
            ..Default::default()
        });

        let desc2 = Some(shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(200),
            range_stats: Some(RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            }),
            ..Default::default()
        });

        let merged = merge_field_descriptors(vec![desc1, desc2]).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();

        assert_eq!(result.position_count, 300);
        assert_eq!(result.null_count, Some(300));

        // Should have range_stats but with None values
        assert!(result.range_stats.is_some());
        let range_stats = result.range_stats.unwrap();
        assert!(range_stats.min_value.is_none());
        assert!(range_stats.max_value.is_none());
    }

    #[test]
    fn test_merge_field_descriptors_mixed_range_stats_presence() {
        // Test scenario where some descriptors have range_stats and others don't

        // Descriptor 1: Has range stats with actual values
        let desc1 = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(50),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(15)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        // Descriptor 2: Has range stats with actual values
        let desc2 = Some(shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(75),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(10)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(30)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        // Descriptor 3: Has range stats but with None min/max
        let desc3 = Some(shard::FieldDescriptor {
            position_count: 150,
            null_count: Some(150),
            range_stats: Some(RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            }),
            ..Default::default()
        });

        let merged = merge_field_descriptors(vec![desc1, desc2, desc3]).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();

        assert_eq!(result.position_count, 450); // 100 + 200 + 150
        assert_eq!(result.null_count, Some(275)); // 50 + 75 + 150

        // Should have range_stats with values from all descriptors
        assert!(result.range_stats.is_some());
        let range_stats = result.range_stats.unwrap();

        // Should have the overall min/max from all descriptors
        assert!(range_stats.min_value.is_some());
        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 5); // min(5, 10, None) = 5
        } else {
            panic!("Expected I64Value for min");
        }

        assert!(range_stats.max_value.is_some());
        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 30); // max(15, 30, None) = 30
        } else {
            panic!("Expected I64Value for max");
        }
    }

    #[test]
    fn test_merge_field_descriptors_only_max_value_present() {
        // Test edge case where some descriptors have only min or only max values

        // Descriptor 1: Has only min value
        let desc1 = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(25),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: None, // No max
                max_inclusive: false,
            }),
            ..Default::default()
        });

        // Descriptor 2: Has only max value
        let desc2 = Some(shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(50),
            range_stats: Some(RangeStats {
                min_value: None, // No min
                min_inclusive: false,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(25)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        });

        let merged = merge_field_descriptors(vec![desc1, desc2]).unwrap();

        assert!(merged.is_some());
        let result = merged.unwrap();

        assert_eq!(result.position_count, 300);
        assert_eq!(result.null_count, Some(75));

        // Should have range_stats with min from desc1 and max from desc2
        assert!(result.range_stats.is_some());
        let range_stats = result.range_stats.unwrap();

        // Should have min from desc1
        assert!(range_stats.min_value.is_some());
        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 5);
        } else {
            panic!("Expected I64Value for min");
        }

        // Should have max from desc2
        assert!(range_stats.max_value.is_some());
        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 25);
        } else {
            panic!("Expected I64Value for max");
        }
    }

    #[test]
    fn test_null_and_nan_count_merging_with_none_values() {
        // Test that if ANY descriptor has None for null_count or nan_count,
        // the merged result should be None

        // Test case 1: One descriptor has None null_count, other has Some
        let mut shard_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: None,
            nan_count: Some(1),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 30,
            null_count: Some(5),
            nan_count: Some(2),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        assert_eq!(shard_field.position_count, 80); // 50 + 30
        assert_eq!(shard_field.null_count, None); // None + Some(5) = None
        assert_eq!(shard_field.nan_count, Some(3)); // Some(1) + Some(2) = Some(3)

        // Test case 2: Both descriptors have Some values
        let mut shard_field2 = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(3),
            nan_count: Some(1),
            ..Default::default()
        };

        let stripe_field2 = shard::FieldDescriptor {
            position_count: 30,
            null_count: Some(5),
            nan_count: Some(2),
            ..Default::default()
        };

        merge(&mut shard_field2, &stripe_field2).unwrap();

        assert_eq!(shard_field2.position_count, 80); // 50 + 30
        assert_eq!(shard_field2.null_count, Some(8)); // Some(3) + Some(5) = Some(8)
        assert_eq!(shard_field2.nan_count, Some(3)); // Some(1) + Some(2) = Some(3)

        // Test case 3: One descriptor has None nan_count, other has Some
        let mut shard_field3 = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(3),
            nan_count: None,
            ..Default::default()
        };

        let stripe_field3 = shard::FieldDescriptor {
            position_count: 30,
            null_count: Some(5),
            nan_count: Some(2),
            ..Default::default()
        };

        merge(&mut shard_field3, &stripe_field3).unwrap();

        assert_eq!(shard_field3.position_count, 80); // 50 + 30
        assert_eq!(shard_field3.null_count, Some(8)); // Some(3) + Some(5) = Some(8)
        assert_eq!(shard_field3.nan_count, None); // None + Some(2) = None
    }

    #[test]
    fn test_ascii_count_merging_with_none_values() {
        // Test that if ANY descriptor has None for ascii_count, the merged result should be None

        let mut shard_field = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(5),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 2,
                    min_non_empty_size: Some(3),
                    max_size: 10,
                    ascii_count: None, // This has None
                },
            )),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(2),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 1,
                    min_non_empty_size: Some(2),
                    max_size: 15,
                    ascii_count: Some(40), // This has Some(40)
                },
            )),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        // Verify that ascii_count is None because shard_field had None
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(merged_stats)) =
            &shard_field.type_specific
        {
            assert_eq!(merged_stats.min_size, 1); // min(2, 1)
            assert_eq!(merged_stats.min_non_empty_size, Some(2)); // min(3, 2)
            assert_eq!(merged_stats.max_size, 15); // max(10, 15)
            assert_eq!(merged_stats.ascii_count, None); // None + Some(40) = None
        } else {
            panic!("Expected string statistics in type_specific field");
        }

        // Test the reverse case: accumulated has Some, current has None
        let mut shard_field2 = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(5),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 2,
                    min_non_empty_size: Some(3),
                    max_size: 10,
                    ascii_count: Some(80), // This has Some(80)
                },
            )),
            ..Default::default()
        };

        let stripe_field2 = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(2),
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 1,
                    min_non_empty_size: Some(2),
                    max_size: 15,
                    ascii_count: None, // This has None
                },
            )),
            ..Default::default()
        };

        merge(&mut shard_field2, &stripe_field2).unwrap();

        // Verify that ascii_count is None because stripe_field2 had None
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(merged_stats)) =
            &shard_field2.type_specific
        {
            assert_eq!(merged_stats.ascii_count, None); // Some(80) + None = None
        } else {
            panic!("Expected string statistics in type_specific field");
        }
    }

    #[test]
    fn test_range_stats_none_is_sticky() {
        // Test that once range_stats becomes None due to a None current, it stays None

        // Start with a descriptor that has range stats
        let mut accumulated = shard::FieldDescriptor {
            position_count: 100,
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(5)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(15)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        // Merge with a descriptor that has None range_stats
        let current_none = shard::FieldDescriptor {
            position_count: 50,
            range_stats: None, // This should disable range stats
            ..Default::default()
        };

        merge(&mut accumulated, &current_none).unwrap();

        // After merging with None, range_stats should be None
        assert!(accumulated.range_stats.is_none());

        // Now try to merge with another descriptor that has range stats
        let current_with_stats = shard::FieldDescriptor {
            position_count: 75,
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(1)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(20)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        merge(&mut accumulated, &current_with_stats).unwrap();

        // Even after merging with a descriptor that has range stats,
        // accumulated.range_stats should still be None (None is sticky)
        assert!(accumulated.range_stats.is_none());

        // But position count should still be aggregated normally
        assert_eq!(accumulated.position_count, 225); // 100 + 50 + 75
    }

    #[test]
    fn test_range_stats_initial_none_then_some() {
        // Test that starting with None and encountering Some sets the range stats

        let mut accumulated = shard::FieldDescriptor {
            position_count: 0,
            range_stats: None, // Start with None (no data yet)
            ..Default::default()
        };

        // Merge with a descriptor that has range stats
        let current_with_stats = shard::FieldDescriptor {
            position_count: 50,
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(10)),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::I64Value(30)),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        merge(&mut accumulated, &current_with_stats).unwrap();

        // Should now have range stats
        assert!(accumulated.range_stats.is_some());
        let range_stats = accumulated.range_stats.unwrap();

        if let Some(Kind::I64Value(min)) = &range_stats.min_value.unwrap().kind {
            assert_eq!(*min, 10);
        } else {
            panic!("Expected I64Value for min");
        }

        if let Some(Kind::I64Value(max)) = &range_stats.max_value.unwrap().kind {
            assert_eq!(*max, 30);
        } else {
            panic!("Expected I64Value for max");
        }
    }

    #[test]
    fn test_type_specific_stats_initial_state_behavior() {
        // Test that type-specific statistics follow the initial state logic

        // Case 1: Accumulated is in initial state - should allow setting type_specific
        let mut accumulated = shard::FieldDescriptor {
            position_count: 0, // Initial state
            type_specific: None,
            ..Default::default()
        };

        let current = shard::FieldDescriptor {
            position_count: 50,
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 5,
                    min_non_empty_size: Some(5),
                    max_size: 20,
                    ascii_count: Some(80),
                },
            )),
            ..Default::default()
        };

        merge(&mut accumulated, &current).unwrap();

        // Should now have type_specific stats because accumulated was in initial state
        assert!(accumulated.type_specific.is_some());
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(stats)) =
            &accumulated.type_specific
        {
            assert_eq!(stats.min_size, 5);
            assert_eq!(stats.max_size, 20);
            assert_eq!(stats.ascii_count, Some(80));
        } else {
            panic!("Expected StringStats");
        }

        // Case 2: Accumulated is NOT in initial state - should NOT allow setting type_specific
        let mut accumulated_not_initial = shard::FieldDescriptor {
            position_count: 100, // Not initial state
            type_specific: None,
            ..Default::default()
        };

        let current2 = shard::FieldDescriptor {
            position_count: 50,
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 3,
                    min_non_empty_size: Some(3),
                    max_size: 15,
                    ascii_count: Some(60),
                },
            )),
            ..Default::default()
        };

        merge(&mut accumulated_not_initial, &current2).unwrap();

        // Should still have None for type_specific because accumulated was not in initial state
        assert!(accumulated_not_initial.type_specific.is_none());
        assert_eq!(accumulated_not_initial.position_count, 150); // 100 + 50
    }

    #[test]
    fn test_merge_container_statistics() {
        // Test merging of ContainerStats (used for binary, list, and map types)
        let mut shard_field = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(5),
            type_specific: Some(shard::field_descriptor::TypeSpecific::ContainerStats(
                shard::ContainerStats {
                    min_length: 5,
                    min_non_empty_length: Some(7),
                    max_length: 50,
                },
            )),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(2),
            type_specific: Some(shard::field_descriptor::TypeSpecific::ContainerStats(
                shard::ContainerStats {
                    min_length: 2,                 // Lower than shard min (5)
                    min_non_empty_length: Some(3), // Lower than shard min_non_empty (7)
                    max_length: 75,                // Higher than shard max (50)
                },
            )),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        // Verify aggregated statistics
        assert_eq!(shard_field.position_count, 150); // 100 + 50
        assert_eq!(shard_field.null_count, Some(7)); // 5 + 2

        // Verify merged container statistics
        if let Some(shard::field_descriptor::TypeSpecific::ContainerStats(merged_stats)) =
            &shard_field.type_specific
        {
            assert_eq!(merged_stats.min_length, 2); // min(5, 2)
            assert_eq!(merged_stats.min_non_empty_length, Some(3)); // min(7, 3)
            assert_eq!(merged_stats.max_length, 75); // max(50, 75)
        } else {
            panic!("Expected container statistics in type_specific field");
        }
    }

    #[test]
    fn test_merge_container_statistics_with_none_min_non_empty() {
        // Test container statistics merging when min_non_empty_length is None in one descriptor
        let mut accumulated = shard::FieldDescriptor {
            position_count: 100,
            type_specific: Some(shard::field_descriptor::TypeSpecific::ContainerStats(
                shard::ContainerStats {
                    min_length: 0,
                    min_non_empty_length: None, // No non-empty containers yet
                    max_length: 10,
                },
            )),
            ..Default::default()
        };

        let current = shard::FieldDescriptor {
            position_count: 50,
            type_specific: Some(shard::field_descriptor::TypeSpecific::ContainerStats(
                shard::ContainerStats {
                    min_length: 1,
                    min_non_empty_length: Some(5), // Has non-empty containers
                    max_length: 20,
                },
            )),
            ..Default::default()
        };

        merge(&mut accumulated, &current).unwrap();

        if let Some(shard::field_descriptor::TypeSpecific::ContainerStats(merged_stats)) =
            &accumulated.type_specific
        {
            assert_eq!(merged_stats.min_length, 0); // min(0, 1)
            assert_eq!(merged_stats.min_non_empty_length, Some(5)); // None + Some(5) = Some(5)
            assert_eq!(merged_stats.max_length, 20); // max(10, 20)
        } else {
            panic!("Expected container statistics in type_specific field");
        }
    }

    #[test]
    fn test_ascii_count_initial_state_behavior() {
        // Test that ascii_count follows the initial state logic like null_count and nan_count

        // Case 1: Accumulated is in initial state - should allow setting ascii_count
        let mut accumulated = shard::FieldDescriptor {
            position_count: 0, // Initial state
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 0,
                    min_non_empty_size: None,
                    max_size: 0,
                    ascii_count: None, // Initially None
                },
            )),
            ..Default::default()
        };

        let current = shard::FieldDescriptor {
            position_count: 50,
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 5,
                    min_non_empty_size: Some(5),
                    max_size: 20,
                    ascii_count: Some(80), // Has Some value
                },
            )),
            ..Default::default()
        };

        merge(&mut accumulated, &current).unwrap();

        // Should now have ascii_count because accumulated was in initial state
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(stats)) =
            &accumulated.type_specific
        {
            assert_eq!(stats.ascii_count, Some(80)); // Should be set from current
        } else {
            panic!("Expected StringStats");
        }

        // Case 2: Accumulated is NOT in initial state - should NOT allow setting ascii_count
        let mut accumulated_not_initial = shard::FieldDescriptor {
            position_count: 100, // Not initial state
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 3,
                    min_non_empty_size: Some(3),
                    max_size: 15,
                    ascii_count: None, // None, but not in initial state
                },
            )),
            ..Default::default()
        };

        let current2 = shard::FieldDescriptor {
            position_count: 50,
            type_specific: Some(shard::field_descriptor::TypeSpecific::StringStats(
                shard::StringStats {
                    min_size: 1,
                    min_non_empty_size: Some(1),
                    max_size: 10,
                    ascii_count: Some(60), // Has Some value
                },
            )),
            ..Default::default()
        };

        merge(&mut accumulated_not_initial, &current2).unwrap();

        // Should still have None for ascii_count because accumulated was not in initial state
        if let Some(shard::field_descriptor::TypeSpecific::StringStats(stats)) =
            &accumulated_not_initial.type_specific
        {
            assert_eq!(stats.ascii_count, None); // Should remain None
        } else {
            panic!("Expected StringStats");
        }
    }

    #[test]
    fn test_merge_boolean_statistics() {
        let mut shard_field = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(5),
            type_specific: Some(shard::field_descriptor::TypeSpecific::BooleanStats(
                shard::BooleanStats {
                    true_count: 60,
                    false_count: 35,
                },
            )),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(2),
            type_specific: Some(shard::field_descriptor::TypeSpecific::BooleanStats(
                shard::BooleanStats {
                    true_count: 30,
                    false_count: 18,
                },
            )),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        assert_eq!(shard_field.position_count, 150);
        assert_eq!(shard_field.null_count, Some(7));

        if let Some(shard::field_descriptor::TypeSpecific::BooleanStats(merged_stats)) =
            &shard_field.type_specific
        {
            assert_eq!(merged_stats.true_count, 90); // 60 + 30
            assert_eq!(merged_stats.false_count, 53); // 35 + 18
        } else {
            panic!("Expected boolean statistics after merge");
        }
    }
    #[test]
    fn test_create_with_stats_boolean_statistics() {
        let boolean_stats = amudai_data_stats::boolean::BooleanStats {
            count: 10,
            null_count: 2,
            true_count: 6,
            false_count: 2,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Boolean(boolean_stats)),
        };

        let descriptor = create_with_stats(10, &encoded_field);

        assert_eq!(descriptor.position_count, 10);
        assert_eq!(descriptor.null_count, Some(2));

        // Check boolean-specific statistics
        if let Some(shard::field_descriptor::TypeSpecific::BooleanStats(proto_boolean_stats)) =
            &descriptor.type_specific
        {
            assert_eq!(proto_boolean_stats.true_count, 6);
            assert_eq!(proto_boolean_stats.false_count, 2);
        } else {
            panic!("Expected boolean statistics in type_specific field");
        }
    }
    #[test]
    fn test_populate_boolean_statistics() {
        let boolean_stats = amudai_data_stats::boolean::BooleanStats {
            count: 8,
            null_count: 1,
            true_count: 5,
            false_count: 2,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Boolean(boolean_stats)),
        };

        let mut descriptor = shard::FieldDescriptor {
            position_count: 8,
            ..Default::default()
        };

        populate_statistics(&mut descriptor, &encoded_field);

        assert_eq!(descriptor.null_count, Some(1));
        assert!(descriptor.type_specific.is_some());

        if let Some(shard::field_descriptor::TypeSpecific::BooleanStats(proto_boolean_stats)) =
            &descriptor.type_specific
        {
            assert_eq!(proto_boolean_stats.true_count, 5);
            assert_eq!(proto_boolean_stats.false_count, 2);
        } else {
            panic!("Expected boolean statistics in type_specific field");
        }
    }

    #[test]
    fn test_boolean_statistics_inconsistency_with_none_pattern() {
        // This test demonstrates that boolean statistics don't follow the
        // "sticky None" pattern that other statistics follow (null_count, nan_count, ascii_count)

        // Test 1: Boolean statistics cannot be None, so they always merge by addition
        let mut shard_field = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10), // This follows sticky None pattern
            type_specific: Some(shard::field_descriptor::TypeSpecific::BooleanStats(
                shard::BooleanStats {
                    true_count: 60,
                    false_count: 30,
                },
            )),
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: None, // This should make merged null_count = None (sticky None)
            type_specific: Some(shard::field_descriptor::TypeSpecific::BooleanStats(
                shard::BooleanStats {
                    true_count: 20,
                    false_count: 30,
                },
            )),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        // null_count follows sticky None pattern - becomes None when either is None
        assert_eq!(
            shard_field.null_count, None,
            "null_count should become None (sticky None pattern)"
        );

        // But boolean statistics always merge by addition (inconsistent behavior)
        if let Some(shard::field_descriptor::TypeSpecific::BooleanStats(bool_stats)) =
            &shard_field.type_specific
        {
            assert_eq!(
                bool_stats.true_count, 80,
                "Boolean true_count always merges by addition"
            ); // 60 + 20
            assert_eq!(
                bool_stats.false_count, 60,
                "Boolean false_count always merges by addition"
            ); // 30 + 30
        } else {
            panic!("Expected boolean statistics");
        }

        // This demonstrates the architectural inconsistency:
        // - null_count, nan_count, ascii_count can be None and follow sticky None pattern
        // - Boolean true_count/false_count are primitive u64 and always mergeable
        //
        // For consistency, boolean statistics should either:
        // 1. Follow the sticky None pattern (require protobuf changes), OR
        // 2. All statistics should be primitive and always mergeable (major architectural change)
    }
}
