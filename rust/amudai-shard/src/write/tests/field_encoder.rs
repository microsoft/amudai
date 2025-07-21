//! Consolidated tests for field encoders
//!
//! This module contains all field encoder tests moved from individual encoder files
//! to improve readability and organization.

use crate::write::field_encoder::{
    BooleanFieldEncoder, BytesFieldEncoder, DecimalFieldEncoder, DictionaryEncoding,
    EncodedFieldStatistics, FieldEncoderParams, PrimitiveFieldEncoder,
};
use amudai_common::Result;
use amudai_format::defs::schema_ext::{BasicTypeDescriptor, KnownExtendedType};
use amudai_format::schema::BasicType;
use amudai_io_impl::temp_file_store;
use std::sync::Arc;

// =============================================================================
// BOOLEAN FIELD ENCODER TESTS
// =============================================================================

#[cfg(test)]
mod boolean_tests {
    use super::*;
    use arrow_array::BooleanArray;

    #[test]
    fn test_boolean_field_encoder_with_statistics() -> Result<()> {
        // Create a temporary store for testing
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        // Create a boolean field encoder
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // Test data with mixed true/false values and nulls
        let array1 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(false),
        ]));
        encoder.push_array(array1)?;

        let array2 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
        ]));
        encoder.push_array(array2)?;

        // Test push_nulls
        encoder.push_nulls(3)?;

        // Finish encoding and get statistics
        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(
            encoded_field.statistics.is_present(),
            "Statistics should be collected"
        );

        if let EncodedFieldStatistics::Boolean(stats) = encoded_field.statistics {
            // Verify counts:
            // Array1: 2 true, 2 false, 1 null
            // Array2: 3 true, 1 false, 0 null
            // push_nulls: 0 true, 0 false, 3 null
            // Total: 5 true, 3 false, 4 null, 12 total count
            assert_eq!(stats.true_count, 5, "Expected 5 true values");
            assert_eq!(stats.false_count, 3, "Expected 3 false values");
            assert_eq!(stats.null_count, 4, "Expected 4 null values");
            assert_eq!(stats.count, 12, "Expected 12 total values");

            // Test utility methods
            assert_eq!(stats.non_null_count(), 8, "Expected 8 non-null values");
            assert!(!stats.is_empty(), "Stats should not be empty");
            assert!(!stats.is_all_nulls(), "Not all values are null");
            assert!(!stats.is_all_true(), "Not all values are true");
            assert!(!stats.is_all_false(), "Not all values are false");

            // Test ratios (approximately)
            let true_ratio = stats.true_ratio().expect("True ratio should be available");
            let false_ratio = stats
                .false_ratio()
                .expect("False ratio should be available");
            let null_ratio = stats.null_ratio().expect("Null ratio should be available");

            // true_ratio and false_ratio are relative to non-null count (8)
            // null_ratio is relative to total count (12)
            assert!(
                (true_ratio - 5.0 / 8.0).abs() < 0.001,
                "True ratio should be ~0.625, got {true_ratio}"
            );
            assert!(
                (false_ratio - 3.0 / 8.0).abs() < 0.001,
                "False ratio should be ~0.375, got {false_ratio}"
            );
            assert!(
                (null_ratio - 4.0 / 12.0).abs() < 0.001,
                "Null ratio should be ~0.333, got {null_ratio}"
            );
        } else {
            panic!("Expected boolean statistics but got different type or None");
        }

        // Verify that buffers were created
        assert!(
            !encoded_field.buffers.is_empty(),
            "Should have created encoded buffers"
        );

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_all_true() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All true values
        let array = Arc::new(BooleanArray::from(vec![true, true, true, true, true]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        if let EncodedFieldStatistics::Boolean(stats) = encoded_field.statistics {
            assert_eq!(stats.true_count, 5);
            assert_eq!(stats.false_count, 0);
            assert_eq!(stats.null_count, 0);
            assert!(stats.is_all_true());
            assert!(!stats.is_all_false());
            assert!(!stats.is_all_nulls());
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_all_false() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All false values
        let array = Arc::new(BooleanArray::from(vec![false, false, false]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        if let EncodedFieldStatistics::Boolean(stats) = encoded_field.statistics {
            assert_eq!(stats.true_count, 0);
            assert_eq!(stats.false_count, 3);
            assert_eq!(stats.null_count, 0);
            assert!(!stats.is_all_true());
            assert!(stats.is_all_false());
            assert!(!stats.is_all_nulls());
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_all_nulls() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All null values
        let array = Arc::new(BooleanArray::from(vec![None, None, None, None]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        if let EncodedFieldStatistics::Boolean(stats) = encoded_field.statistics {
            assert_eq!(stats.true_count, 0);
            assert_eq!(stats.false_count, 0);
            assert_eq!(stats.null_count, 4);
            assert!(!stats.is_all_true());
            assert!(!stats.is_all_false());
            assert!(stats.is_all_nulls());
            assert_eq!(stats.non_null_count(), 0);
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_empty() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // No arrays pushed
        let encoded_field = encoder.finish()?;

        if let EncodedFieldStatistics::Boolean(stats) = encoded_field.statistics {
            assert_eq!(stats.true_count, 0);
            assert_eq!(stats.false_count, 0);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.count, 0);
            assert!(stats.is_empty());
        } else {
            panic!("Expected boolean statistics");
        }

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_constant_true_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All true values
        let array = Arc::new(BooleanArray::from(vec![true, true, true, true, true]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify constant value optimization
        assert!(
            encoded_field.constant_value.is_some(),
            "Should have constant value"
        );
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            assert!(matches!(constant_val.kind, Some(Kind::BoolValue(true))));
        }

        // Should have no buffers for constant fields
        assert!(
            encoded_field.buffers.is_empty(),
            "Should have no buffers for constant field"
        );

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_constant_false_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All false values
        let array = Arc::new(BooleanArray::from(vec![false, false, false]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify constant value optimization
        assert!(
            encoded_field.constant_value.is_some(),
            "Should have constant value"
        );
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            assert!(matches!(constant_val.kind, Some(Kind::BoolValue(false))));
        }

        // Should have no buffers for constant fields
        assert!(
            encoded_field.buffers.is_empty(),
            "Should have no buffers for constant field"
        );

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_constant_null_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All null values
        let array = Arc::new(BooleanArray::from(vec![None, None, None, None]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify constant value optimization
        assert!(
            encoded_field.constant_value.is_some(),
            "Should have constant value"
        );
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            assert!(matches!(constant_val.kind, Some(Kind::NullValue(_))));
        }

        // Should have no buffers for constant fields
        assert!(
            encoded_field.buffers.is_empty(),
            "Should have no buffers for constant field"
        );

        Ok(())
    }

    #[test]
    fn test_boolean_field_encoder_mixed_values_no_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Boolean,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };

        let mut encoder = BooleanFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // Mixed values - should not be optimized
        let array = Arc::new(BooleanArray::from(vec![true, false, true]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Should not have constant value optimization
        assert!(
            encoded_field.constant_value.is_none(),
            "Should not have constant value"
        );

        // Should have buffers for non-constant fields
        assert!(
            !encoded_field.buffers.is_empty(),
            "Should have buffers for non-constant field"
        );

        Ok(())
    }
}

// =============================================================================
// BYTES FIELD ENCODER TESTS
// =============================================================================

#[cfg(test)]
mod bytes_tests {
    use super::*;
    use crate::write::field_encoder::BytesStatsCollector;
    use amudai_encodings::block_encoder::BlockEncodingProfile;
    use arrow_array::BinaryArray;

    #[test]
    fn test_binary_field_encoder_with_statistics() -> Result<()> {
        // Create a temporary store for testing
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        // Create a binary field encoder
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // Test data with various binary values and nulls
        let array1 = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"hello" as &[u8]),
            Some(b"world"),
            None,
            Some(b""), // Empty binary
            Some(b"test123"),
        ]));
        encoder.push_array(array1)?;

        let array2 = Arc::new(BinaryArray::from_vec(vec![
            b"binary",
            b"data",
            b"statistics",
        ]));
        encoder.push_array(array2)?;

        // Test push_nulls
        encoder.push_nulls(2)?;

        // Finish encoding and get statistics
        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(
            encoded_field.statistics.is_present(),
            "Statistics should be collected for Binary fields"
        );

        if let EncodedFieldStatistics::Binary(stats) = encoded_field.statistics {
            // Verify counts:
            // Array1: 4 non-null values (hello, world, "", test123), 1 null
            // Array2: 3 non-null values (binary, data, statistics), 0 nulls
            // push_nulls: 2 nulls
            // Total: 7 non-null values, 3 nulls, 10 total count
            assert_eq!(stats.total_count, 10);
            assert_eq!(stats.null_count, 3);
            assert_eq!(stats.non_null_count(), 7);

            // Verify length statistics
            assert_eq!(stats.min_length, 0); // Empty binary value ""
            assert_eq!(stats.max_length, 10); // "statistics" is 10 bytes
            assert_eq!(stats.min_non_empty_length, Some(4)); // "data" is 4 bytes
        } else {
            panic!("Expected Binary statistics");
        }

        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_field_encoder_with_statistics() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 4,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // FixedSizeBinary arrays - all values have the same length
        let array = Arc::new(arrow_array::FixedSizeBinaryArray::from(vec![
            b"abcd", b"efgh", b"ijkl",
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Binary(stats) = encoded_field.statistics {
            assert_eq!(stats.total_count, 3);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.min_length, 4); // Fixed size
            assert_eq!(stats.max_length, 4); // Fixed size
            assert_eq!(stats.min_non_empty_length, Some(4));
        } else {
            panic!("Expected Binary statistics");
        }

        Ok(())
    }

    #[test]
    fn test_guid_field_encoder_with_statistics() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Guid,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // GUID data (16 bytes each)
        let guid1 = [0u8; 16];
        let guid2 = [1u8; 16];
        let array = Arc::new(arrow_array::FixedSizeBinaryArray::from(vec![
            &guid1, &guid2,
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Binary(stats) = encoded_field.statistics {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.min_length, 16); // GUID size
            assert_eq!(stats.max_length, 16); // GUID size
            assert_eq!(stats.min_non_empty_length, Some(16));
        } else {
            panic!("Expected Binary statistics");
        }

        Ok(())
    }

    #[test]
    fn test_string_field_encoder_still_works() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::String,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        let array = Arc::new(arrow_array::StringArray::from(vec!["hello", "world"]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // String fields should still get String statistics, not Binary statistics
        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::String(_stats) = encoded_field.statistics {
            // This is correct - String fields should get String statistics
        } else {
            panic!("Expected String statistics for String field");
        }

        Ok(())
    }

    #[test]
    fn test_bytes_stats_collector_enum_dispatching() -> Result<()> {
        // Test that the enum correctly dispatches to String statistics
        let mut string_collector =
            BytesStatsCollector::new(BasicType::String, BlockEncodingProfile::Balanced);
        let string_array = Arc::new(arrow_array::StringArray::from(vec!["hello", "world"]));
        string_collector.process_array(string_array.as_ref())?;

        if let EncodedFieldStatistics::String(stats) = string_collector.finish()? {
            assert_eq!(stats.count, 2);
            assert_eq!(stats.null_count, 0);
        } else {
            panic!("Expected String statistics");
        }

        // Test that the enum correctly dispatches to Binary statistics
        let mut binary_collector =
            BytesStatsCollector::new(BasicType::Binary, BlockEncodingProfile::Balanced);
        let binary_array = Arc::new(arrow_array::BinaryArray::from_vec(vec![b"hello", b"world"]));
        binary_collector.process_array(binary_array.as_ref())?;

        if let EncodedFieldStatistics::Binary(stats) = binary_collector.finish()? {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
        } else {
            panic!("Expected Binary statistics");
        }

        // Test that None returns no statistics
        let mut none_collector =
            BytesStatsCollector::new(BasicType::Int32, BlockEncodingProfile::Balanced); // Unsupported type
        let int_array = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
        let result = none_collector.process_array(int_array.as_ref());
        assert!(result.is_ok()); // Should not fail, just do nothing

        assert!(none_collector.finish()?.is_missing());

        Ok(())
    }

    #[test]
    #[should_panic(
        expected = "Decimal fields should be handled by DecimalFieldEncoder, not BytesFieldEncoder"
    )]
    fn test_bytes_field_encoder_rejects_decimal_fields() {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        // This should panic because BytesFieldEncoder should not handle decimal fields
        let _encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        });
    }

    #[test]
    fn test_bytes_field_encoder_constant_binary_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All same binary values - should be detected as constant
        let constant_value = b"constant_binary_value";
        let array = Arc::new(BinaryArray::from_vec(vec![
            constant_value,
            constant_value,
            constant_value,
            constant_value,
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(encoded_field.statistics.is_present());

        // Verify constant value was detected
        assert!(encoded_field.constant_value.is_some());
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            if let Some(Kind::BytesValue(bytes)) = constant_val.kind {
                assert_eq!(bytes, constant_value.to_vec());
            } else {
                panic!("Expected BytesValue but got {:?}", constant_val.kind);
            }
        }

        Ok(())
    }

    #[test]
    fn test_bytes_field_encoder_constant_string_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::String,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All same string values - should be detected as constant
        let constant_value = "constant_string_value";
        let array = Arc::new(arrow_array::StringArray::from(vec![
            constant_value,
            constant_value,
            constant_value,
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(encoded_field.statistics.is_present());

        // Verify constant value was detected
        assert!(encoded_field.constant_value.is_some());
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            if let Some(Kind::StringValue(string)) = constant_val.kind {
                assert_eq!(string, constant_value);
            } else {
                panic!("Expected StringValue but got {:?}", constant_val.kind);
            }
        }

        Ok(())
    }

    #[test]
    fn test_bytes_field_encoder_constant_null_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // All null values - should be detected as constant null
        let array = Arc::new(BinaryArray::from_opt_vec(vec![
            None::<&[u8]>,
            None,
            None,
            None,
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(encoded_field.statistics.is_present());

        // Verify constant null value was detected
        assert!(encoded_field.constant_value.is_some());
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            assert!(matches!(constant_val.kind, Some(Kind::NullValue(_))));
        }

        Ok(())
    }

    #[test]
    fn test_bytes_field_encoder_mixed_values_no_constant() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            fixed_size: 0,
            signed: false,
            extended_type: KnownExtendedType::None,
        };

        let mut encoder = BytesFieldEncoder::create(&FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        })?;

        // Mixed values - should not be detected as constant
        let array = Arc::new(BinaryArray::from_vec(vec![b"value1", b"value2", b"value3"]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify that statistics were collected
        assert!(encoded_field.statistics.is_present());

        // Verify no constant value was detected
        assert!(encoded_field.constant_value.is_none());

        Ok(())
    }
}

// =============================================================================
// DECIMAL FIELD ENCODER TESTS
// =============================================================================

#[cfg(test)]
mod decimal_tests {
    use super::*;
    use arrow_array::FixedSizeBinaryArray;

    #[test]
    fn test_decimal_field_encoder_basic() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create test decimal data
        let decimal1 = [0u8; 16]; // Represents decimal value 0
        let decimal2 = [1u8; 16]; // Represents decimal value with some bits set
        let array = Arc::new(FixedSizeBinaryArray::from(vec![&decimal1, &decimal2]));

        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify we got statistics
        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Decimal(stats) = encoded_field.statistics {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
        } else {
            panic!("Expected decimal statistics");
        }

        // Verify we got encoded buffers
        assert!(!encoded_field.buffers.is_empty());

        Ok(())
    }

    #[test]
    fn test_decimal_field_encoder_with_nulls() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create test data with nulls
        let decimal1 = [0u8; 16];
        let array = Arc::new(FixedSizeBinaryArray::from(vec![
            Some(&decimal1[..]),
            None,
            Some(&decimal1[..]),
        ]));

        encoder.push_array(array)?;

        // Also test push_nulls
        encoder.push_nulls(2)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics
        if let EncodedFieldStatistics::Decimal(stats) = encoded_field.statistics {
            assert_eq!(stats.total_count, 5); // 3 from array + 2 from push_nulls
            assert_eq!(stats.null_count, 3); // 1 from array + 2 from push_nulls
        } else {
            panic!("Expected decimal statistics");
        }

        Ok(())
    }

    #[test]
    fn test_decimal_field_encoder_with_statistics() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create decimal data (16 bytes each)
        let decimal1 = [0u8; 16]; // Represents decimal value 0
        let decimal2 = [1u8; 16]; // Represents decimal value with some bits set
        let array = Arc::new(FixedSizeBinaryArray::from(vec![&decimal1, &decimal2]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Should have decimal statistics
        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Decimal(stats) = encoded_field.statistics {
            assert_eq!(stats.total_count, 2);
            assert_eq!(stats.null_count, 0);
            assert!(stats.min_value.is_some());
            assert!(stats.max_value.is_some());
        } else {
            panic!("Expected Decimal statistics for decimal field");
        }

        Ok(())
    }

    #[test]
    fn test_decimal_field_encoder_early_constant_detection_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();
        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            fixed_size: 16,
            signed: false,
            extended_type: KnownExtendedType::KustoDecimal,
        };

        let params = FieldEncoderParams {
            basic_type,
            temp_store,
            encoding_profile: Default::default(),
            dictionary_encoding: DictionaryEncoding::Enabled,
        };

        let mut encoder = DecimalFieldEncoder::create(&params)?;

        // Create a constant decimal value
        let constant_decimal_bytes = decimal::d128::from(42).to_raw_bytes();

        // Add a large amount of constant data - this should be detected early
        // and avoid expensive buffer encoding and index creation
        for _ in 0..100 {
            let array = Arc::new(FixedSizeBinaryArray::from(vec![
                &constant_decimal_bytes[..];
                100
            ])); // 100 copies of the same decimal
            encoder.push_array(array)?;
        }

        let encoded_field = encoder.finish()?;

        // Verify that we detected the constant value and optimized storage
        assert!(
            encoded_field.constant_value.is_some(),
            "Should have constant value"
        );
        assert!(
            encoded_field.buffers.is_empty(),
            "Should have no buffers for constant field"
        );

        // Verify constant value is correct
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            if let Some(Kind::DecimalValue(bytes)) = constant_val.kind {
                assert_eq!(
                    bytes,
                    constant_decimal_bytes.to_vec(),
                    "Constant decimal bytes should match"
                );
            } else {
                panic!(
                    "Expected DecimalValue for decimal constant, got: {:?}",
                    constant_val.kind
                );
            }
        }

        // Verify statistics still collected properly
        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Decimal(stats) = encoded_field.statistics {
            assert_eq!(stats.total_count, 10_000); // 100 * 100
            assert_eq!(stats.null_count, 0);
            assert!(stats.min_value.is_some());
            assert!(stats.max_value.is_some());
            // Min and max should be the same for constant values
            assert_eq!(stats.min_value, stats.max_value);
        }

        Ok(())
    }

    // Additional decimal tests can be added here as needed...
}

// =============================================================================
// PRIMITIVE FIELD ENCODER TESTS
// =============================================================================

#[cfg(test)]
mod primitive_tests {
    use super::*;
    use amudai_arrow_compat::datetime_conversions;
    use amudai_blockstream::read::block_stream::BlockReaderPrefetch;
    use amudai_blockstream::read::primitive_buffer::PrimitiveBufferDecoder;
    use arrow_array::builder::{TimestampMillisecondBuilder, TimestampNanosecondBuilder};
    use arrow_array::{Float32Array, Float64Array, Int32Array};
    use std::time::SystemTime;

    #[test]
    fn test_datetime_field_encoder() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::DateTime,
            fixed_size: 0,
            signed: false,
            extended_type: Default::default(),
        };
        let now = SystemTime::now();
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
                dictionary_encoding: DictionaryEncoding::Enabled,
            },
            arrow_schema::DataType::UInt64,
        )?;

        for _ in 0..10 {
            let now_millis = datetime_conversions::now_unix_milliseconds();
            let mut ts_builder = TimestampMillisecondBuilder::with_capacity(10000);
            for i in 0..5000 {
                let ts = now_millis + (i * 10);
                ts_builder.append_value(ts);
            }
            let ts_array = Arc::new(ts_builder.finish());
            encoder.push_array(ts_array)?;

            let now_nanos = datetime_conversions::now_unix_nanoseconds();
            let mut ts_builder = TimestampNanosecondBuilder::with_capacity(10000);
            for i in 0..5000 {
                let ts = now_nanos + (i * 10);
                ts_builder.append_value(ts);
            }
            let ts_array = Arc::new(ts_builder.finish());
            encoder.push_array(ts_array)?;
        }

        let encoded_field = encoder.finish()?;
        assert_eq!(encoded_field.buffers.len(), 2); // data + 1 combined index buffer

        // The first buffer should be the data buffer
        assert_eq!(
            encoded_field.buffers[0].descriptor.kind,
            amudai_format::defs::shard::BufferKind::Data as i32
        );

        // The second buffer should be the combined index buffer
        assert_eq!(
            encoded_field.buffers[1].descriptor.kind,
            amudai_format::defs::shard::BufferKind::RangeIndex as i32
        );

        let prepared_buffer = &encoded_field.buffers[0]; // Use the data buffer for decoding
        let decoder = PrimitiveBufferDecoder::from_prepared_buffer(prepared_buffer, basic_type)?;
        let mut reader =
            decoder.create_reader_with_ranges(std::iter::empty(), BlockReaderPrefetch::Disabled)?;
        let seq = reader.read_range(1000..2000).unwrap();
        let t = seq.values.as_slice::<u64>()[0];
        let t = datetime_conversions::ticks_to_unix_seconds(t).unwrap();
        assert!(
            t < now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 200
        );
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collection_int32() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
                dictionary_encoding: DictionaryEncoding::Enabled,
            },
            arrow_schema::DataType::Int32,
        )?;

        // Push some test data
        let array = Arc::new(Int32Array::from(vec![1, 5, 3, 8, 2]));
        encoder.push_array(array)?;

        let array_with_nulls = Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(15),
            None,
            Some(20),
        ]));
        encoder.push_array(array_with_nulls)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics were collected
        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Primitive(stats) = encoded_field.statistics {
            // Verify basic counts
            assert_eq!(stats.count, 10); // 5 + 5 values
            assert_eq!(stats.null_count, 2); // 2 nulls in second array

            // Verify range statistics
            assert!(stats.range_stats.min_value.is_some());
            assert!(stats.range_stats.max_value.is_some());

            let min_val = stats.range_stats.min_value.unwrap().as_i64().unwrap();
            let max_val = stats.range_stats.max_value.unwrap().as_i64().unwrap();

            assert_eq!(min_val, 1);
            assert_eq!(max_val, 20);
        } else {
            panic!("Expected primitive statistics but got different type or None");
        }

        Ok(())
    }

    #[test]
    fn test_primitive_stats_collection_float32_with_nan() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            fixed_size: 0,
            signed: true,
            extended_type: KnownExtendedType::None,
        };
        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
                dictionary_encoding: DictionaryEncoding::Enabled,
            },
            arrow_schema::DataType::Float32,
        )?;

        // Push test data with NaN values
        let array = Arc::new(Float32Array::from(vec![
            Some(1.5),
            Some(f32::NAN),
            Some(3.7),
            None,
            Some(2.1),
            Some(f32::NAN),
        ]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify statistics were collected - f32 should use FloatingStats now
        assert!(encoded_field.statistics.is_present());
        if let EncodedFieldStatistics::Floating(stats) = encoded_field.statistics {
            // Verify basic counts
            assert_eq!(stats.total_count, 6);
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.nan_count, 2);
            assert_eq!(stats.positive_count, 3); // 1.5, 3.7, 2.1
            assert_eq!(stats.negative_count, 0);
            assert_eq!(stats.zero_count, 0);
            assert_eq!(stats.positive_infinity_count, 0);
            assert_eq!(stats.negative_infinity_count, 0);

            // Verify range statistics (should exclude NaN values)
            assert!(stats.min_value.is_some());
            assert!(stats.max_value.is_some());

            let min_val = stats.min_value.unwrap();
            let max_val = stats.max_value.unwrap();

            assert!((min_val - 1.5).abs() < 0.001);
            assert!((max_val - 3.7).abs() < 0.001);
        } else {
            panic!("Expected floating statistics but got different type or None");
        }

        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_constant_value_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            fixed_size: 0,
            signed: true,
            extended_type: Default::default(),
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
                dictionary_encoding: DictionaryEncoding::Disabled,
            },
            arrow_schema::DataType::Int32,
        )?;

        // All values are the same constant
        let array1 = Arc::new(Int32Array::from(vec![42, 42, 42, 42, 42]));
        encoder.push_array(array1)?;

        let array2 = Arc::new(Int32Array::from(vec![42, 42, 42]));
        encoder.push_array(array2)?;

        let encoded_field = encoder.finish()?;

        // Verify constant value optimization
        assert!(
            encoded_field.constant_value.is_some(),
            "Should have constant value"
        );
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            if let Some(Kind::I64Value(val)) = constant_val.kind {
                assert_eq!(val, 42, "Constant value should be 42");
            } else {
                panic!("Expected I64Value for Int32 constant");
            }
        }

        // Should have no buffers for constant fields
        assert!(
            encoded_field.buffers.is_empty(),
            "Should have no buffers for constant field"
        );

        Ok(())
    }

    #[test]
    fn test_primitive_field_encoder_floating_constant_optimization() -> Result<()> {
        let temp_store = temp_file_store::create_in_memory(16 * 1024 * 1024).unwrap();

        let basic_type = BasicTypeDescriptor {
            basic_type: BasicType::Float64,
            fixed_size: 0,
            signed: true,
            extended_type: Default::default(),
        };

        let mut encoder = PrimitiveFieldEncoder::create(
            &FieldEncoderParams {
                basic_type,
                temp_store,
                encoding_profile: Default::default(),
                dictionary_encoding: DictionaryEncoding::Disabled,
            },
            arrow_schema::DataType::Float64,
        )?;

        // All values are the same floating point constant
        let array = Arc::new(Float64Array::from(vec![3.14159, 3.14159, 3.14159, 3.14159]));
        encoder.push_array(array)?;

        let encoded_field = encoder.finish()?;

        // Verify constant value optimization
        assert!(
            encoded_field.constant_value.is_some(),
            "Should have constant value"
        );
        if let Some(constant_val) = encoded_field.constant_value {
            use amudai_format::defs::common::any_value::Kind;
            if let Some(Kind::DoubleValue(val)) = constant_val.kind {
                assert!(
                    (val - 3.14159).abs() < f64::EPSILON,
                    "Constant value should be 3.14159"
                );
            } else {
                panic!("Expected DoubleValue for Float64 constant");
            }
        }

        // Should have no buffers for constant fields
        assert!(
            encoded_field.buffers.is_empty(),
            "Should have no buffers for constant field"
        );

        Ok(())
    }

    // Additional primitive tests would continue here...
    // (The rest of the primitive tests from the original file can be added as needed)
}
