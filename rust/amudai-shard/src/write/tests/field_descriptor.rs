#[cfg(test)]
mod tests {
    use crate::write::field_descriptor::{
        create_with_stats, merge, merge_field_descriptors, populate_statistics,
    };
    use crate::write::field_encoder::{EncodedField, EncodedFieldStatistics};

    use amudai_common::Result;
    use amudai_data_stats::primitive::PrimitiveStats;
    use amudai_format::defs::common::{AnyValue, any_value::Kind};
    use amudai_format::defs::schema_ext::BasicTypeDescriptor;
    use amudai_format::defs::shard::{self, RangeStats};

    #[test]
    fn test_create_with_stats_no_statistics() {
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: None,
            dictionary_size: None,
        };
        let descriptor = create_with_stats(100, &encoded_field);

        assert_eq!(descriptor.position_count, 100);
        assert_eq!(descriptor.null_count, None);
        assert!(descriptor.range_stats.is_none());
    }

    #[test]
    fn test_create_with_stats_primitive_statistics() {
        let primitive_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 50,
            null_count: 5,
            raw_data_size: 181, // 45 non-null values * 4 bytes + ceil(50/8) = 1 byte null bitmap = 181 bytes
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
            dictionary_size: Some(10),
        };

        let descriptor = create_with_stats(50, &encoded_field);

        assert_eq!(descriptor.position_count, 50);
        assert_eq!(descriptor.null_count, Some(5));
        assert!(descriptor.range_stats.is_some());
        assert_eq!(descriptor.dictionary_size, Some(10));

        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_some());
        assert!(range_stats.max_value.is_some());
    }

    #[test]
    fn test_populate_statistics_primitive_zero_nan_count() {
        let primitive_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 50,
            null_count: 5,
            raw_data_size: 181, // 45 non-null values * 4 bytes + ceil(50/8) = 1 byte null bitmap = 181 bytes
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
            dictionary_size: None,
        };

        let mut descriptor = shard::FieldDescriptor {
            position_count: 50,
            ..Default::default()
        };
        populate_statistics(&mut descriptor, &encoded_field);

        assert_eq!(descriptor.null_count, Some(5));
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
            range_stats: None,
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(5),
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
            raw_data_size: 75, // Variable length string data + null bitmap overhead
            bloom_filter: None,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::String(string_stats)),
            dictionary_size: None,
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
            raw_data_size: 2500, // Variable length string data, no null bitmap since no nulls
            bloom_filter: None,
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::String(string_stats)),
            dictionary_size: None,
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
    fn test_floating_point_vs_integer_stats_handling() {
        // Test that floating-point types use FloatingStats while integers use PrimitiveStats
        use amudai_data_stats::floating::FloatingStats;

        let floating_stats = FloatingStats {
            total_count: 51,
            null_count: 0,
            zero_count: 10,
            positive_count: 20,
            negative_count: 15,
            nan_count: 3,
            positive_infinity_count: 1,
            negative_infinity_count: 2,
            min_value: None,
            max_value: None,
            raw_data_size: 408, // 51 float64 values * 8 bytes = 408 bytes (no nulls, so no null bitmap)
        };

        // Test Float32 with FloatingStats
        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Floating(floating_stats.clone())),
            dictionary_size: None,
        };
        populate_statistics(&mut descriptor, &encoded_field);

        // Check that type_specific contains FloatingStats
        if let Some(shard::field_descriptor::TypeSpecific::FloatingStats(stats)) =
            &descriptor.type_specific
        {
            assert_eq!(stats.nan_count, 3);
            assert_eq!(stats.positive_infinity_count, 1);
            assert_eq!(stats.negative_infinity_count, 2);
        } else {
            panic!("Expected FloatingStats in type_specific");
        }

        // Test Int32 - should not have FloatingStats
        let int32_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 100,
            null_count: 5,
            raw_data_size: 393, // 95 non-null values * 4 bytes + ceil(100/8) = 13 bytes null bitmap = 393 bytes
            range_stats: RangeStats {
                min_value: None,
                min_inclusive: false,
                max_value: None,
                max_inclusive: false,
            },
        };

        let mut descriptor = shard::FieldDescriptor::default();
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(int32_stats)),
            dictionary_size: None,
        };
        populate_statistics(&mut descriptor, &encoded_field);

        // Should not have FloatingStats for integer types
        assert!(descriptor.type_specific.is_none());
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
            null_count: 10,   // All nulls
            raw_data_size: 0, // All null values = 0 bytes as per spec
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
            raw_data_size: 66, // 8 non-null values * 8 bytes + ceil(10/8) = 2 bytes null bitmap = 66 bytes
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
            dictionary_size: None,
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
            dictionary_size: None,
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
        } // Test with one Some descriptor mixed with None values - should return None
        let desc = Some(shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
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
    fn test_null_count_merging_with_none_values() {
        // Test that if ANY descriptor has None for null_count,
        // the merged result should be None

        // Test case 1: One descriptor has None null_count, other has Some
        let mut shard_field = shard::FieldDescriptor {
            position_count: 50,
            null_count: None,
            ..Default::default()
        };

        let stripe_field = shard::FieldDescriptor {
            position_count: 30,
            null_count: Some(5),
            ..Default::default()
        };

        merge(&mut shard_field, &stripe_field).unwrap();

        assert_eq!(shard_field.position_count, 80); // 50 + 30
        assert_eq!(shard_field.null_count, None); // None + Some(5) = None

        // Test case 2: Both descriptors have Some values
        let mut shard_field2 = shard::FieldDescriptor {
            position_count: 50,
            null_count: Some(3),
            ..Default::default()
        };

        let stripe_field2 = shard::FieldDescriptor {
            position_count: 30,
            null_count: Some(5),
            ..Default::default()
        };

        merge(&mut shard_field2, &stripe_field2).unwrap();

        assert_eq!(shard_field2.position_count, 80); // 50 + 30
        assert_eq!(shard_field2.null_count, Some(8)); // Some(3) + Some(5) = Some(8)
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
        // Test that ascii_count follows the initial state logic like null_count

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
            raw_data_size: 4, // ceil(10/8) = 2 bytes for boolean data + ceil(10/8) = 2 bytes null bitmap = 4 bytes
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Boolean(boolean_stats)),
            dictionary_size: None,
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
            raw_data_size: 2, // ceil(8/8) = 1 byte for boolean data + ceil(8/8) = 1 byte null bitmap = 2 bytes
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Boolean(boolean_stats)),
            dictionary_size: None,
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
        // "sticky None" pattern that other statistics follow (null_count, ascii_count)

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
        // - null_count, ascii_count can be None and follow sticky None pattern
        // - Boolean true_count/false_count are primitive u64 and always mergeable
        //
        // For consistency, boolean statistics should either:
        // 1. Follow the sticky None pattern (require protobuf changes), OR
        // 2. All statistics should be primitive and always mergeable (major architectural change)
    }
    #[test]
    fn test_populate_decimal_statistics() {
        use decimal::d128;
        use std::str::FromStr; // Create test decimal statistics
        let decimal_stats = amudai_data_stats::decimal::DecimalStats {
            min_value: Some(d128::from_str("123.45").unwrap()),
            max_value: Some(d128::from_str("987.65").unwrap()),
            null_count: 5,
            total_count: 100,
            raw_data_size: 1533, // 95 non-null values * 16 bytes + ceil(100/8) = 13 bytes null bitmap = 1533 bytes
            zero_count: 10,
            positive_count: 80,
            negative_count: 5,
            nan_count: 0,
        };

        // Create an encoded field with decimal statistics
        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Decimal(decimal_stats)),
            dictionary_size: None,
        };

        // Create a field descriptor and populate it
        let mut descriptor = amudai_format::defs::shard::FieldDescriptor::default();
        populate_statistics(&mut descriptor, &encoded_field);

        // Verify the statistics were populated correctly
        assert_eq!(descriptor.null_count, Some(5));

        // Verify range statistics
        assert!(descriptor.range_stats.is_some());
        let range_stats = descriptor.range_stats.unwrap();
        assert!(range_stats.min_value.is_some());
        assert!(range_stats.max_value.is_some());
        assert!(range_stats.min_inclusive);
        assert!(range_stats.max_inclusive);
    }

    #[test]
    fn test_decimal_statistics_merging_with_binary_representation() -> Result<()> {
        use amudai_format::defs::common::{AnyValue, any_value::Kind};
        use decimal::d128;
        use std::str::FromStr;

        // Create two field descriptors with decimal range statistics
        let mut descriptor1 = shard::FieldDescriptor {
            position_count: 100,
            null_count: Some(10),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::DecimalValue(
                        d128::from_str("100.50").unwrap().to_raw_bytes().to_vec(),
                    )),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::DecimalValue(
                        d128::from_str("500.75").unwrap().to_raw_bytes().to_vec(),
                    )),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        let descriptor2 = shard::FieldDescriptor {
            position_count: 200,
            null_count: Some(15),
            range_stats: Some(RangeStats {
                min_value: Some(AnyValue {
                    kind: Some(Kind::DecimalValue(
                        d128::from_str("25.25").unwrap().to_raw_bytes().to_vec(), // Lower than descriptor1 min
                    )),
                    annotation: None,
                }),
                min_inclusive: true,
                max_value: Some(AnyValue {
                    kind: Some(Kind::DecimalValue(
                        d128::from_str("750.99").unwrap().to_raw_bytes().to_vec(), // Higher than descriptor1 max
                    )),
                    annotation: None,
                }),
                max_inclusive: true,
            }),
            ..Default::default()
        };

        // Merge descriptor2 into descriptor1
        merge(&mut descriptor1, &descriptor2)?;

        // Verify the merged results
        assert_eq!(descriptor1.position_count, 300); // 100 + 200
        assert_eq!(descriptor1.null_count, Some(25)); // 10 + 15

        // Verify that the min value is the lower one (25.25)
        let range_stats = descriptor1.range_stats.unwrap();
        assert!(range_stats.min_value.is_some());
        assert!(range_stats.max_value.is_some());

        // Extract and verify the min value
        if let Some(Kind::DecimalValue(min_bytes)) = &range_stats.min_value.unwrap().kind {
            let mut array = [0u8; 16];
            array.copy_from_slice(min_bytes);
            let min_decimal = unsafe { d128::from_raw_bytes(array) };
            assert_eq!(min_decimal, d128::from_str("25.25").unwrap());
        } else {
            panic!("Expected DecimalValue for min");
        }

        // Extract and verify the max value
        if let Some(Kind::DecimalValue(max_bytes)) = &range_stats.max_value.unwrap().kind {
            let mut array = [0u8; 16];
            array.copy_from_slice(max_bytes);
            let max_decimal = unsafe { d128::from_raw_bytes(array) };
            assert_eq!(max_decimal, d128::from_str("750.99").unwrap());
        } else {
            panic!("Expected DecimalValue for max");
        }

        Ok(())
    }

    #[test]
    fn test_decimal_comparison_string_vs_binary() {
        use amudai_format::defs::anyvalue_ext::AnyValueExt;
        use amudai_format::defs::common::{AnyValue, any_value::Kind};
        use decimal::d128;
        use std::str::FromStr;

        // Test case: "9.99" vs "10.0"
        // String comparison: "9.99" > "10.0" (incorrect lexicographic)
        // Numeric comparison: 9.99 < 10.0 (correct)

        let decimal1 = d128::from_str("9.99").unwrap();
        let decimal2 = d128::from_str("10.0").unwrap();

        // Old approach: String-based comparison (incorrect)
        let string_value1 = AnyValue {
            kind: Some(Kind::StringValue("9.99".to_string())),
            annotation: None,
        };
        let string_value2 = AnyValue {
            kind: Some(Kind::StringValue("10.0".to_string())),
            annotation: None,
        };

        // New approach: Binary-based comparison (correct)
        let binary_value1 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal1.to_raw_bytes().to_vec())),
            annotation: None,
        };
        let binary_value2 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal2.to_raw_bytes().to_vec())),
            annotation: None,
        };

        // String comparison gives wrong result (9.99 > 10.0 lexicographically)
        let string_comparison = string_value1.compare(&string_value2).unwrap();
        assert_eq!(string_comparison, 1); // "9.99" > "10.0" - WRONG!

        // Binary comparison gives correct result (9.99 < 10.0 numerically)
        let binary_comparison = binary_value1.compare(&binary_value2).unwrap();
        assert_eq!(binary_comparison, -1); // 9.99 < 10.0 - CORRECT!

        // Test another case: "2.0" vs "10.0"
        let decimal3 = d128::from_str("2.0").unwrap();
        let decimal4 = d128::from_str("10.0").unwrap();

        let string_value3 = AnyValue {
            kind: Some(Kind::StringValue("2.0".to_string())),
            annotation: None,
        };
        let string_value4 = AnyValue {
            kind: Some(Kind::StringValue("10.0".to_string())),
            annotation: None,
        };

        let binary_value3 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal3.to_raw_bytes().to_vec())),
            annotation: None,
        };
        let binary_value4 = AnyValue {
            kind: Some(Kind::DecimalValue(decimal4.to_raw_bytes().to_vec())),
            annotation: None,
        };

        // String comparison: "2.0" > "10.0" (wrong again!)
        let string_comparison2 = string_value3.compare(&string_value4).unwrap();
        assert_eq!(string_comparison2, 1); // "2.0" > "10.0" - WRONG!

        // Binary comparison: 2.0 < 10.0 (correct)
        let binary_comparison2 = binary_value3.compare(&binary_value4).unwrap();
        assert_eq!(binary_comparison2, -1); // 2.0 < 10.0 - CORRECT!
    }

    #[test]
    fn test_raw_data_size_population_and_merging() {
        // Test that raw_data_size is properly populated for all statistics types

        // Test primitive statistics
        let primitive_stats = PrimitiveStats {
            basic_type: BasicTypeDescriptor {
                basic_type: amudai_format::schema::BasicType::Int32,
                signed: true,
                fixed_size: 0,
                extended_type: Default::default(),
            },
            count: 100,
            null_count: 10,
            raw_data_size: 365, // 90 non-null values * 4 bytes + ceil(100/8) = 13 bytes null bitmap = 365 bytes
            range_stats: RangeStats {
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
            },
        };

        let encoded_field = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::Primitive(primitive_stats)),
            dictionary_size: None,
        };

        let descriptor = create_with_stats(100, &encoded_field);
        assert_eq!(descriptor.raw_data_size, Some(365));

        // Test string statistics
        let string_stats = amudai_data_stats::string::StringStats {
            min_size: 5,
            min_non_empty_size: Some(5),
            max_size: 20,
            ascii_count: 80,
            count: 100,
            null_count: 20,
            raw_data_size: 1213, // Variable length string data + null bitmap overhead
            bloom_filter: None,
        };

        let encoded_field2 = EncodedField {
            buffers: vec![],
            statistics: Some(EncodedFieldStatistics::String(string_stats)),
            dictionary_size: None,
        };

        let descriptor2 = create_with_stats(100, &encoded_field2);
        assert_eq!(descriptor2.raw_data_size, Some(1213));

        // Test merging - raw_data_size should be aggregated
        let mut accumulated = descriptor.clone();
        merge(&mut accumulated, &descriptor2).unwrap();

        assert_eq!(accumulated.position_count, 200); // 100 + 100
        assert_eq!(accumulated.raw_data_size, Some(1578)); // 365 + 1213
    }
}
