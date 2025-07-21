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

    descriptor.dictionary_size = encoded_field.dictionary_size;
    descriptor.constant_value = encoded_field.constant_value.clone();
    descriptor
}

/// Populates an existing field descriptor with statistics from the encoded field.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate with statistics
/// * `encoded_field` - The encoded field containing optional statistics
pub fn populate_statistics(descriptor: &mut shard::FieldDescriptor, encoded_field: &EncodedField) {
    match &encoded_field.statistics {
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
        EncodedFieldStatistics::Decimal(decimal_stats) => {
            populate_decimal_statistics(descriptor, decimal_stats);
        }
        EncodedFieldStatistics::Floating(floating_stats) => {
            populate_floating_statistics(descriptor, floating_stats);
        }
        EncodedFieldStatistics::Missing => {
            // No statistics to populate
        }
    }
}

/// Merges statistics from one field descriptor into another field descriptor.
///
/// This function aggregates statistics from two field descriptors by combining their
/// values appropriately. It handles:
/// - Position count aggregation
/// - Null count aggregation (None if either is None)
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
        // This is the first stripe for this field. After cloning its descriptor,
        // we must clear the membership filters, as shard-level descriptors
        // must not contain them.
        accumulated.membership_filters = None;
        // Also clear dictionary size from the shard-level descriptor.
        accumulated.dictionary_size = None;
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

    // Merge raw data size statistics
    // If any descriptor has None for raw_data_size, the result should be None
    match (accumulated.raw_data_size, current.raw_data_size) {
        (Some(accumulated_raw_data_size), Some(current_raw_data_size)) => {
            accumulated.raw_data_size = Some(accumulated_raw_data_size + current_raw_data_size);
        }
        (_, _) => {
            // If either is None, result is None
            accumulated.raw_data_size = None;
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

    // Merge constant value - if any stripe doesn't have a constant value, or has a different
    // constant value, the shard-level constant value should be None
    match (&accumulated.constant_value, &current.constant_value) {
        (Some(accumulated_constant), Some(current_constant)) => {
            // Both stripes have constant values - check if they are equal
            if accumulated_constant != current_constant {
                // Different constant values across stripes - field is not constant at shard level
                accumulated.constant_value = None;
            }
            // If they're equal, keep the existing value
        }
        (None, Some(_)) => {
            // Accumulated has no constant value (from previous stripes),
            // so shard-level field is not constant
        }
        (Some(_), None) => {
            // Current stripe has no constant value, so shard-level field is not constant
            accumulated.constant_value = None;
        }
        (None, None) => {
            // Neither has constant values, keep as None
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
                // Accumulated has None, this maintains consistency with null_count behavior
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

    // Map raw data size
    descriptor.raw_data_size = Some(primitive_stats.raw_data_size);

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

    // Map raw data size
    descriptor.raw_data_size = Some(string_stats.raw_data_size);

    // Set string-specific statistics in type_specific field
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::StringStats(
        shard::StringStats {
            min_size: string_stats.min_size,
            min_non_empty_size: string_stats.min_non_empty_size,
            max_size: string_stats.max_size,
            ascii_count: Some(string_stats.ascii_count),
        },
    ));

    // Add bloom filter to membership_filters if one was constructed
    if let Some(ref bloom_filter) = string_stats.bloom_filter {
        descriptor.membership_filters = Some(shard::MembershipFilters {
            sbbf: Some(bloom_filter.clone()),
        });
    }
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

    // Map raw data size
    descriptor.raw_data_size = Some(boolean_stats.raw_data_size);

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

    // Map raw data size
    descriptor.raw_data_size = Some(binary_stats.raw_data_size);

    // Set binary-specific statistics in type_specific field using ContainerStats
    // which is appropriate for binary data (similar to how it's used for lists/maps)
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::ContainerStats(
        shard::ContainerStats {
            min_length: binary_stats.min_length,
            max_length: binary_stats.max_length,
            min_non_empty_length: binary_stats.min_non_empty_length,
        },
    ));

    // Add bloom filter to membership_filters if one was constructed
    if let Some(ref bloom_filter) = binary_stats.bloom_filter {
        descriptor.membership_filters = Some(shard::MembershipFilters {
            sbbf: Some(bloom_filter.clone()),
        });
    }
}

/// Populates a field descriptor with decimal statistics.
///
/// Maps decimal-specific statistics from the data stats collector to the shard
/// field descriptor format. For decimals, we set the null count and use range
/// statistics to track minimum and maximum decimal values using the new
/// DecimalValue variant with 16-byte binary representation.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate
/// * `decimal_stats` - The decimal statistics to copy from
fn populate_decimal_statistics(
    descriptor: &mut shard::FieldDescriptor,
    decimal_stats: &amudai_data_stats::decimal::DecimalStats,
) {
    use amudai_format::defs::common::{AnyValue, any_value::Kind};

    // Map null count
    descriptor.null_count = Some(decimal_stats.null_count);

    // Map raw data size
    descriptor.raw_data_size = Some(decimal_stats.raw_data_size);

    // Convert d128 values to binary representation for storage using DecimalValue
    let min_value = decimal_stats.min_value.as_ref().map(|d| AnyValue {
        annotation: None,
        kind: Some(Kind::DecimalValue(d.to_raw_bytes().to_vec())),
    });

    let max_value = decimal_stats.max_value.as_ref().map(|d| AnyValue {
        annotation: None,
        kind: Some(Kind::DecimalValue(d.to_raw_bytes().to_vec())),
    });

    // Map range statistics (min/max decimal values)
    descriptor.range_stats = Some(shard::RangeStats {
        min_value,
        min_inclusive: true, // Decimal ranges are typically inclusive
        max_value,
        max_inclusive: true, // Decimal ranges are typically inclusive
    });

    // Populate type-specific decimal statistics
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::DecimalStats(
        shard::DecimalStats {
            zero_count: decimal_stats.zero_count,
            positive_count: decimal_stats.positive_count,
            negative_count: decimal_stats.negative_count,
            nan_count: decimal_stats.nan_count,
        },
    ));
}

/// Populates field descriptor with floating-point statistics.
///
/// This function converts floating-point statistics from the data stats format to the
/// field descriptor format. For floating-point values, we set the null count and use range
/// statistics to track minimum and maximum values. The floating-point specific counts
/// are stored in the FloatingStats type-specific statistics.
///
/// # Arguments
///
/// * `descriptor` - The field descriptor to populate
/// * `floating_stats` - The floating-point statistics to copy from
fn populate_floating_statistics(
    descriptor: &mut shard::FieldDescriptor,
    floating_stats: &amudai_data_stats::floating::FloatingStats,
) {
    use amudai_format::defs::common::{AnyValue, any_value::Kind};

    // Map null count
    descriptor.null_count = Some(floating_stats.null_count);

    // Map raw data size
    descriptor.raw_data_size = Some(floating_stats.raw_data_size);

    // Convert f64 values to AnyValue for storage
    let min_value = floating_stats.min_value.map(|v| AnyValue {
        annotation: None,
        kind: Some(Kind::DoubleValue(v)),
    });

    let max_value = floating_stats.max_value.map(|v| AnyValue {
        annotation: None,
        kind: Some(Kind::DoubleValue(v)),
    });

    // Map range statistics (min/max floating-point values, excluding NaN)
    descriptor.range_stats = Some(shard::RangeStats {
        min_value,
        min_inclusive: true, // Floating-point ranges are typically inclusive
        max_value,
        max_inclusive: true, // Floating-point ranges are typically inclusive
    });

    // Populate type-specific floating-point statistics
    descriptor.type_specific = Some(shard::field_descriptor::TypeSpecific::FloatingStats(
        shard::FloatingStats {
            zero_count: floating_stats.zero_count,
            positive_count: floating_stats.positive_count,
            negative_count: floating_stats.negative_count,
            nan_count: floating_stats.nan_count,
            positive_infinity_count: floating_stats.positive_infinity_count,
            negative_infinity_count: floating_stats.negative_infinity_count,
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
        (
            TypeSpecific::DecimalStats(accumulated_stats),
            TypeSpecific::DecimalStats(current_stats),
        ) => {
            // Merge decimal statistics by summing counts
            accumulated_stats.zero_count += current_stats.zero_count;
            accumulated_stats.positive_count += current_stats.positive_count;
            accumulated_stats.negative_count += current_stats.negative_count;
            accumulated_stats.nan_count += current_stats.nan_count;
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
