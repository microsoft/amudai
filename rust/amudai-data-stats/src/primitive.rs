use amudai_arrow_compat::any_value_support::ToAnyValue;
use amudai_common::Result;
use amudai_format::{defs::shard::RangeStats, schema::BasicTypeDescriptor};
use arrow_array::{
    Array, ArrowNativeTypeOp, ArrowPrimitiveType,
    cast::AsArray,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
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
            amudai_format::schema::BasicType::Float32 => {
                Box::new(FloatRangeStatsCollector::<Float32Type>::new())
            }
            amudai_format::schema::BasicType::Float64 => {
                Box::new(FloatRangeStatsCollector::<Float64Type>::new())
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
            nan_count: self.range_stats.get_nan_count(),
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
    pub nan_count: usize,
}

/// A trait for collecting range statistics.
trait RangeStatsCollector: Send + Sync {
    fn get_count(&self) -> usize;
    fn get_nan_count(&self) -> usize;
    fn get_null_count(&self) -> usize;
    fn finalize(&mut self) -> RangeStats;
    fn update(&mut self, values: &dyn Array);
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

    /// Returns the count of `NaN` values processed.
    ///
    /// # Returns
    ///
    /// The count of `NaN` values.
    fn get_nan_count(&self) -> usize {
        0
    }

    /// Returns the count of null values processed.
    ///
    /// # Returns
    ///
    /// The count of null values.
    fn get_null_count(&self) -> usize {
        self.null_count
    }
}

/// An implementation of `RangeStatsCollector` for FP types.
struct FloatRangeStatsCollector<T: ArrowPrimitiveType> {
    min: Option<T::Native>,
    max: Option<T::Native>,
    count: usize,
    null_count: usize,
    nan_count: usize,
}

impl<T: ArrowPrimitiveType> FloatRangeStatsCollector<T> {
    /// Creates a new instance of `FloatRangeStatsCollector`.
    ///
    /// # Returns
    ///
    /// A new instance of `FloatRangeStatsCollector`.
    pub fn new() -> Self {
        Self {
            max: None,
            min: None,
            count: 0,
            null_count: 0,
            nan_count: 0,
        }
    }
}

impl<T: ArrowPrimitiveType> RangeStatsCollector for FloatRangeStatsCollector<T>
where
    T::Native: ToAnyValue + FloatStatsOps,
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
        self.nan_count += typed_values.iter().flatten().filter(|v| v.is_nan()).count();

        let min = typed_values
            .iter()
            .flatten()
            .filter(|v| !v.is_nan())
            .min_by(|a, b| a.compare(*b));
        self.min = min_of(self.min, min);

        let max = typed_values
            .iter()
            .flatten()
            .filter(|v| !v.is_nan())
            .max_by(|a, b| a.compare(*b));
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

    /// Returns the count of `NaN` values processed.
    ///
    /// # Returns
    ///
    /// The count of `NaN` values.
    fn get_nan_count(&self) -> usize {
        self.nan_count
    }

    /// Returns the count of null values processed.
    ///
    /// # Returns
    ///
    /// The count of null values.
    fn get_null_count(&self) -> usize {
        self.null_count
    }
}

trait FloatStatsOps {
    fn is_nan(&self) -> bool;
}

impl FloatStatsOps for f32 {
    fn is_nan(&self) -> bool {
        f32::is_nan(*self)
    }
}

impl FloatStatsOps for f64 {
    fn is_nan(&self) -> bool {
        f64::is_nan(*self)
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
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };

    use super::PrimitiveStatsCollector;

    #[test]
    fn test_simple_primitive_stats_collector() {
        let stats = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
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
    fn test_primitive_stats_collector_float32_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            signed: true,
            fixed_size: 0,
        });
        let array = Float32Array::from(vec![Some(1.1), None, Some(3.3), Some(4.4), None]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(
            stats.range_stats.min_value.unwrap().as_f64().unwrap(),
            1.1f32 as f64
        );
        assert_eq!(
            stats.range_stats.max_value.unwrap().as_f64().unwrap(),
            4.4f32 as f64
        );
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_float64_with_nulls() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Float64,
            signed: true,
            fixed_size: 0,
        });
        let array = Float64Array::from(vec![
            Some(1.11f64),
            None,
            Some(3.33f64),
            Some(4.44f64),
            None,
        ]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_f64().unwrap(), 1.11);
        assert_eq!(stats.range_stats.max_value.unwrap().as_f64().unwrap(), 4.44);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 2);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_float32_with_nan() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Float32,
            signed: true,
            fixed_size: 0,
        });
        let array = Float32Array::from(vec![
            Some(1.1),
            None,
            Some(3.3),
            Some(f32::NAN),
            Some(4.4),
            None,
        ]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(
            stats.range_stats.min_value.unwrap().as_f64().unwrap(),
            1.1f32 as f64
        );

        assert_eq!(
            stats.range_stats.max_value.unwrap().as_f64().unwrap(),
            4.4f32 as f64
        );
        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.nan_count, 1);
        Ok(())
    }

    #[test]
    fn test_primitive_stats_collector_float64_with_nan() -> Result<()> {
        let mut collector = PrimitiveStatsCollector::new(BasicTypeDescriptor {
            basic_type: BasicType::Float64,
            signed: true,
            fixed_size: 0,
        });
        let array = Float64Array::from(vec![
            Some(1.11f64),
            None,
            Some(3.33f64),
            Some(f64::NAN),
            Some(4.44f64),
            None,
        ]);
        collector.process_array(&array)?;
        let stats = collector.finish()?;
        assert_eq!(stats.range_stats.min_value.unwrap().as_f64().unwrap(), 1.11);
        assert_eq!(stats.range_stats.max_value.unwrap().as_f64().unwrap(), 4.44);
        assert_eq!(stats.count, 6);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.nan_count, 1);
        Ok(())
    }
}
