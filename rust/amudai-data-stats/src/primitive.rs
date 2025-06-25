use amudai_arrow_compat::any_value_support::ToAnyValue;
use amudai_common::Result;
use amudai_format::{defs::shard::RangeStats, schema::BasicTypeDescriptor};
use arrow_array::{
    Array, ArrowNativeTypeOp, ArrowPrimitiveType,
    cast::AsArray,
    types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};

/// A struct to collect statistics for primitive types.
pub struct PrimitiveStatsCollector {
    basic_type: BasicTypeDescriptor,
    range_stats: Box<dyn RangeStatsCollector>,
}

impl PrimitiveStatsCollector {
    /// Creates a new `PrimitiveStatsCollector` for the given `BasicTypeDescriptor`.
    ///
    /// # Arguments
    ///
    /// * `basic_type` - A descriptor of the basic type for which statistics will be collected.
    ///
    /// # Returns
    ///
    /// A new instance of `PrimitiveStatsCollector`.
    pub fn new(basic_type: BasicTypeDescriptor) -> PrimitiveStatsCollector {
        let range_stats: Box<dyn RangeStatsCollector> = match basic_type.basic_type {
            amudai_format::schema::BasicType::Int8 => {
                if basic_type.signed {
                    Box::new(IntegerRangeStatsCollector::<Int8Type>::new())
                } else {
                    Box::new(IntegerRangeStatsCollector::<UInt8Type>::new())
                }
            }
            amudai_format::schema::BasicType::Int16 => {
                if basic_type.signed {
                    Box::new(IntegerRangeStatsCollector::<Int16Type>::new())
                } else {
                    Box::new(IntegerRangeStatsCollector::<UInt16Type>::new())
                }
            }
            amudai_format::schema::BasicType::Int32 => {
                if basic_type.signed {
                    Box::new(IntegerRangeStatsCollector::<Int32Type>::new())
                } else {
                    Box::new(IntegerRangeStatsCollector::<UInt32Type>::new())
                }
            }
            amudai_format::schema::BasicType::Int64 => {
                if basic_type.signed {
                    Box::new(IntegerRangeStatsCollector::<Int64Type>::new())
                } else {
                    Box::new(IntegerRangeStatsCollector::<UInt64Type>::new())
                }
            }
            amudai_format::schema::BasicType::DateTime => {
                // DateTime is stored as UInt64 internally
                Box::new(IntegerRangeStatsCollector::<UInt64Type>::new())
            }
            _ => panic!("unexpected type: {:?}", basic_type.basic_type),
        };
        PrimitiveStatsCollector {
            basic_type,
            range_stats,
        }
    }

    /// Processes an array and updates the statistics.
    ///
    /// # Arguments
    ///
    /// * `array` - A reference to an array of values to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn process_array(&mut self, array: &dyn Array) -> Result<()> {
        match array.data_type() {
            arrow_schema::DataType::Int8
            | arrow_schema::DataType::UInt8
            | arrow_schema::DataType::Int16
            | arrow_schema::DataType::UInt16
            | arrow_schema::DataType::Int32
            | arrow_schema::DataType::UInt32
            | arrow_schema::DataType::Int64
            | arrow_schema::DataType::UInt64
            | arrow_schema::DataType::Float32
            | arrow_schema::DataType::Float64 => {}
            _ => panic!("unexpected type: {:?}", array.data_type()),
        }
        self.range_stats.update(array);
        Ok(())
    }

    /// Processes a specified number of null values and updates the statistics.
    ///
    /// This method is more efficient than creating a null array when you only need to
    /// track null counts without processing actual values.
    ///
    /// # Arguments
    ///
    /// * `null_count` - The number of null values to add to the statistics
    pub fn process_nulls(&mut self, null_count: usize) {
        self.range_stats.process_nulls(null_count);
    }

    /// Finalizes the statistics collection and returns the collected statistics.
    ///
    /// # Returns
    ///
    /// A `Result` containing the collected `PrimitiveStats` or an error.
    pub fn finish(mut self) -> Result<PrimitiveStats> {
        let range_stats = self.range_stats.finalize();
        Ok(PrimitiveStats {
            basic_type: self.basic_type,
            range_stats,
            count: self.range_stats.get_count(),
            null_count: self.range_stats.get_null_count(),
        })
    }
}

/// A struct to hold the collected statistics for primitive types.
#[derive(Debug, Clone)]
pub struct PrimitiveStats {
    pub basic_type: BasicTypeDescriptor,
    pub range_stats: RangeStats,
    pub count: usize,
    pub null_count: usize,
}

/// A trait for collecting range statistics.
trait RangeStatsCollector: Send + Sync {
    fn get_count(&self) -> usize;
    fn get_null_count(&self) -> usize;
    fn finalize(&mut self) -> RangeStats;
    fn update(&mut self, values: &dyn Array);
    fn process_nulls(&mut self, null_count: usize);
}

/// An implementation of `RangeStatsCollector` for primitive non-FP types.
struct IntegerRangeStatsCollector<T: ArrowPrimitiveType> {
    min: Option<T::Native>,
    max: Option<T::Native>,
    count: usize,
    null_count: usize,
}

impl<T: ArrowPrimitiveType> IntegerRangeStatsCollector<T> {
    /// Creates a new instance of `IntegerRangeStatsCollector`.
    ///
    /// # Returns
    ///
    /// A new instance of `IntegerRangeStatsCollector`.
    pub fn new() -> Self {
        Self {
            max: None,
            min: None,
            count: 0,
            null_count: 0,
        }
    }
}

impl<T: ArrowPrimitiveType> RangeStatsCollector for IntegerRangeStatsCollector<T>
where
    T::Native: ToAnyValue,
{
    /// Updates the range statistics with the given array of values.
    ///
    /// # Arguments
    ///
    /// * `values` - A reference to an array of values to be processed.
    fn update(&mut self, values: &dyn Array) {
        let typed_values = values.as_primitive::<T>();

        self.count += typed_values.len();
        self.null_count += typed_values.null_count();

        let min = arrow_arith::aggregate::min(typed_values);
        self.min = min_of(self.min, min);

        let max = arrow_arith::aggregate::max(typed_values);
        self.max = max_of(self.max, max);
    }

    /// Finalizes the range statistics collection and returns the collected statistics.
    ///
    /// # Returns
    ///
    /// The collected `RangeStats`.
    fn finalize(&mut self) -> RangeStats {
        let min = self.min.map(|v| v.to_any_value());
        let max = self.max.map(|v| v.to_any_value());
        RangeStats {
            min_value: min,
            min_inclusive: true,
            max_value: max,
            max_inclusive: true,
        }
    }

    /// Returns the total count of values processed.
    ///
    /// # Returns
    ///
    /// The total count of values.
    fn get_count(&self) -> usize {
        self.count
    }
    /// Returns the count of null values processed.
    ///
    /// # Returns
    ///
    /// The count of null values.
    fn get_null_count(&self) -> usize {
        self.null_count
    }

    /// Processes a specified number of null values and updates the statistics.
    ///
    /// This method is more efficient than creating a null array when you only need to
    /// track null counts without processing actual values.
    ///
    /// # Arguments
    ///
    /// * `null_count` - The number of null values to add to the statistics
    fn process_nulls(&mut self, null_count: usize) {
        self.count += null_count;
        self.null_count += null_count;
        // No changes to min/max since nulls don't contribute to range statistics
    }
}

fn min_of<T: ArrowNativeTypeOp>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (None, None) => None,
        (None, Some(_)) => right,
        (Some(_), None) => left,
        (Some(lhs), Some(rhs)) => {
            if lhs < rhs {
                left
            } else {
                right
            }
        }
    }
}

fn max_of<T: ArrowNativeTypeOp>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (None, None) => None,
        (None, Some(_)) => right,
        (Some(_), None) => left,
        (Some(lhs), Some(rhs)) => {
            if lhs < rhs {
                right
            } else {
                left
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use amudai_common::Result;
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use arrow_array::{
        Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };

    use super::PrimitiveStatsCollector;

    #[test]
    fn test_simple_primitive_stats_collector() {
        let stats = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let stats = stats.finish().unwrap();
        assert_eq!(stats.null_count, 0);
    }

    #[test]
    fn test_primitive_stats_collector_int8() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int8Array::from(vec![1, 2, 3, 4, 5]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 5);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int16() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int16Array::from(vec![10, 20, 30, 40, 50]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 10);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 50);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int32() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(vec![100, 200, 300, 400, 500]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 100);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 500);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int64() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int64Array::from(vec![1000, 2000, 3000, 4000, 5000]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1000);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 5000);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint8() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt8Array::from(vec![1, 2, 3, 4, 5]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 5);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint16() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt16Array::from(vec![10, 20, 30, 40, 50]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 10);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 50);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint32() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt32Array::from(vec![100, 200, 300, 400, 500]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 100);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 500);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint64() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt64Array::from(vec![1000, 2000, 3000, 4000, 5000]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1000);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 5000);
        assert_eq!(stats.count, 5);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int8_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int8Array::from(vec![Some(1), None, Some(3), Some(4), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 4);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int16_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int16Array::from(vec![Some(10), None, Some(30), Some(40), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 10);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 40);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int32_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(vec![Some(100), None, Some(300), Some(400), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 100);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 400);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_int64_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int64Array::from(vec![Some(1000), None, Some(3000), Some(4000), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1000);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 4000);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint8_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt8Array::from(vec![Some(1), None, Some(3), Some(4), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 4);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint16_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt16Array::from(vec![Some(10), None, Some(30), Some(40), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 10);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 40);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint32_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt32Array::from(vec![Some(100), None, Some(300), Some(400), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 100);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 400);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_uint64_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt64Array::from(vec![Some(1000), None, Some(3000), Some(4000), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1000);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 4000);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }
    #[test]
    fn test_primitive_stats_collector_process_nulls_method() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(vec![Some(10), None, Some(20)]);
        collector.process_array(&array)?;

        // Add additional nulls using the dedicated method
        collector.process_nulls(3);

        let stats = collector.finish()?;
        assert_eq!(stats.count, 6); // 3 from array + 3 from process_nulls
        assert_eq!(stats.null_count, 4); // 1 from array + 3 from process_nulls
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 10);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 20);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_process_nulls_only() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });

        // Only process nulls without any array data
        collector.process_nulls(5);

        let stats = collector.finish()?;
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 5);
        assert!(stats.range_stats.min_value.is_none()); // No non-null values processed
        assert!(stats.range_stats.max_value.is_none());
        Ok(())
    }
    #[test]
    fn test_primitive_stats_collector_datetime() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::DateTime,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        // DateTime is stored as UInt64 internally (ticks)
        let array = UInt64Array::from(vec![
            637500000000000000u64, // Some timestamp
            637600000000000000u64, // Later timestamp
            637400000000000000u64, // Earlier timestamp
        ]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(
            stats.range_stats.min_value.unwrap().as_i64().unwrap(),
            637400000000000000i64
        );
        assert_eq!(
            stats.range_stats.max_value.unwrap().as_i64().unwrap(),
            637600000000000000i64
        );
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_empty_array() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(Vec::<i32>::new());
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert!(stats.range_stats.min_value.is_none());
        assert!(stats.range_stats.max_value.is_none());
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_all_nulls_array() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(vec![None, None, None, None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 4);
        assert!(stats.range_stats.min_value.is_none());
        assert!(stats.range_stats.max_value.is_none());
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_single_value() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(vec![42]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.count, 1);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 42);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 42);
        assert!(stats.range_stats.min_inclusive);
        assert!(stats.range_stats.max_inclusive);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_boundary_values_int32() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = Int32Array::from(vec![i32::MIN, i32::MAX, 0]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(
            stats.range_stats.min_value.unwrap().as_i64().unwrap(),
            i32::MIN as i64
        );
        assert_eq!(
            stats.range_stats.max_value.unwrap().as_i64().unwrap(),
            i32::MAX as i64
        );
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_boundary_values_uint64() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: false,
            fixed_size: 0,
            extended_type: Default::default(),
        });
        let array = UInt64Array::from(vec![u64::MIN, u64::MAX, 1000]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(
            stats.range_stats.min_value.unwrap().as_i64().unwrap(),
            u64::MIN as i64
        );
        // Note: u64::MAX cannot be represented as i64, but the test verifies conversion
        assert!(stats.range_stats.max_value.is_some());
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_multiple_arrays() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });

        // Process first array
        let array1 = Int32Array::from(vec![1, 2, 3]);
        collector.process_array(&array1)?;

        // Process second array with different range
        let array2 = Int32Array::from(vec![Some(10), None, Some(15)]);
        collector.process_array(&array2)?;

        // Process third array
        let array3 = Int32Array::from(vec![-5, 0]);
        collector.process_array(&array3)?;

        let stats = collector.finish()?;
        assert_eq!(stats.count, 8); // 3 + 3 + 2
        assert_eq!(stats.null_count, 1); // Only one null from array2
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), -5);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 15);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_mixed_operations() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });

        // Start with an array
        let array = Int32Array::from(vec![Some(5), None, Some(10)]);
        collector.process_array(&array)?;

        // Add some nulls
        collector.process_nulls(2);

        // Add another array
        let array2 = Int32Array::from(vec![1, 20]);
        collector.process_array(&array2)?;

        // Add more nulls
        collector.process_nulls(1);

        let stats = collector.finish()?;
        assert_eq!(stats.count, 8); // 3 + 2 + 2 + 1
        assert_eq!(stats.null_count, 4); // 1 + 2 + 0 + 1
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 20);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_process_nulls_zero_count() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });

        let array = Int32Array::from(vec![1, 2, 3]);
        collector.process_array(&array)?;

        // Adding zero nulls should not change anything
        collector.process_nulls(0);

        let stats = collector.finish()?;
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.range_stats.min_value.unwrap().as_i64().unwrap(), 1);
        assert_eq!(stats.range_stats.max_value.unwrap().as_i64().unwrap(), 3);
        Ok(())
    }

    #[test]
    #[should_panic(expected = "unexpected type")]
    fn test_primitive_stats_collector_unsupported_type() {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
            extended_type: Default::default(),
        });

        // Try to process a string array with an int32 collector
        let array = arrow_array::StringArray::from(vec!["hello", "world"]);
        let _ = collector.process_array(&array);
    }
}
