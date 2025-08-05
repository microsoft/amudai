//! Utilities for printing [`Frame`]s and [`Sequence`]s as JSON.
//!
//! Provides traits and implementations to convert values, sequences and frames into `serde_json::Value`.
//! Intended for diagnostics, debugging, and testing purposes only.
//! Not suitable for production, precision-critical or performance-sensitive use.
//! May change or be removed in the future.

use amudai_common::{Result, error::Error, verify_arg};
use amudai_format::schema::BasicType;
use std::ops::Range;

use crate::{
    fixed_list_sequence::FixedListSequence,
    frame::Frame,
    list_sequence::ListSequence,
    map_sequence::MapSequence,
    sequence::{AsSequence, Sequence, ValueSequence},
    struct_sequence::StructSequence,
};

/// Read a single value at a given index and convert it into JSON.
pub trait ReadValueAsJson {
    /// Read the value at `index` and produce a `serde_json::Value`.
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value>;
}

/// Convert an entire [`Frame`] into a JSON array of records.
pub trait FrameToJson {
    /// Produce a `serde_json::Value::Array` of zero or more records.
    ///
    /// If `index_range` is `None`, all records in the frame are included.
    fn to_json(&self, index_range: Option<Range<usize>>) -> Result<serde_json::Value>;
}

/// Implementation of [`FrameToJson`] for [`Frame`].
impl FrameToJson for Frame {
    fn to_json(&self, index_range: Option<Range<usize>>) -> Result<serde_json::Value> {
        let index_range = index_range.unwrap_or(0..self.len());
        let records = index_range
            .map(|i| self.read_value_as_json(i))
            .collect::<Result<Vec<_>>>()?;
        Ok(serde_json::Value::Array(records))
    }
}

/// Implementation of [`ReadValueAsJson`] for any [`Sequence`] trait object.
/// Dispatches based on the underlying `BasicType`.
impl ReadValueAsJson for dyn Sequence {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        let type_desc = self.basic_type();
        match type_desc.basic_type {
            // primitive & simple types
            BasicType::Unit
            | BasicType::Boolean
            | BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid
            | BasicType::DateTime => self.as_value().read_value_as_json(index),

            // compound types
            BasicType::List => self.as_list().read_value_as_json(index),
            BasicType::FixedSizeList => self.as_fixed_list().read_value_as_json(index),
            BasicType::Struct => self.as_struct().read_value_as_json(index),
            BasicType::Map => self.as_map().read_value_as_json(index),

            // not yet supported
            BasicType::Union => todo!(),
        }
    }
}

/// Implementation of [`ReadValueAsJson`] for [`Frame`], producing a JSON object per record.
impl ReadValueAsJson for Frame {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        verify_arg!(index, index < self.len());
        let mut map = serde_json::Map::new();
        for (ordinal, field) in self.fields.iter().enumerate() {
            // derive field name from schema or fallback to "field_{ordinal}"
            let name = if let Some(schema) = &self.schema {
                schema
                    .fields()
                    .get(ordinal)
                    .map(|f| f.name().to_string())
                    .unwrap_or_else(|| format!("field_{ordinal}"))
            } else {
                format!("field_{ordinal}")
            };

            let value = field.as_ref().read_value_as_json(index)?;
            map.insert(name, value);
        }
        Ok(serde_json::Value::Object(map))
    }
}

/// Implementation of [`ReadValueAsJson`] for [`ListSequence`].
impl ReadValueAsJson for ListSequence {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        verify_arg!(index, index < self.len());
        if self.presence.is_null(index) {
            return Ok(serde_json::Value::Null);
        }
        let range = self.offsets.range_at(index);
        let mut array = Vec::new();
        if let Some(item_sequence) = &self.item {
            for item_index in range.start..range.end {
                array.push(
                    item_sequence
                        .as_ref()
                        .read_value_as_json(item_index as usize)?,
                );
            }
        }
        Ok(serde_json::Value::Array(array))
    }
}

/// Implementation of [`ReadValueAsJson`] for [`FixedListSequence`].
impl ReadValueAsJson for FixedListSequence {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        verify_arg!(index, index < self.len());
        if self.presence.is_null(index) {
            return Ok(serde_json::Value::Null);
        }
        let mut array = Vec::new();
        if let Some(item_sequence) = &self.item {
            let start = index * self.list_size;
            for i in start..start + self.list_size {
                array.push(item_sequence.as_ref().read_value_as_json(i)?);
            }
        }
        Ok(serde_json::Value::Array(array))
    }
}

/// Implementation of [`ReadValueAsJson`] for [`StructSequence`].
impl ReadValueAsJson for StructSequence {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        verify_arg!(index, index < self.len());
        if self.presence.is_null(index) {
            return Ok(serde_json::Value::Null);
        }
        let mut map = serde_json::Map::new();
        for (ordinal, field) in self.fields.iter().enumerate() {
            let name = self
                .data_type
                .as_ref()
                .and_then(|dt| dt.children().get(ordinal))
                .map(|c| c.name().to_string())
                .unwrap_or_else(|| format!("_{ordinal}"));

            map.insert(name, field.as_ref().read_value_as_json(index)?);
        }
        Ok(serde_json::Value::Object(map))
    }
}

/// Implementation of [`ReadValueAsJson`] for [`MapSequence`].
impl ReadValueAsJson for MapSequence {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        verify_arg!(index, index < self.len());
        if self.presence.is_null(index) {
            return Ok(serde_json::Value::Null);
        }
        let range = self.offsets.range_at(index);
        let mut map = serde_json::Map::new();
        match (&self.key, &self.value) {
            (Some(k), Some(v)) => {
                for (i, idx) in (range.start..range.end).enumerate() {
                    let key = k.as_ref().read_value_as_json(idx as usize)?;
                    let key_str = match key {
                        serde_json::Value::String(s) => s,
                        _ => format!("key_{i}"),
                    };
                    map.insert(key_str, v.as_ref().read_value_as_json(idx as usize)?);
                }
            }
            (None, Some(v)) => {
                for (i, idx) in (range.start..range.end).enumerate() {
                    map.insert(
                        format!("key_{i}"),
                        v.as_ref().read_value_as_json(idx as usize)?,
                    );
                }
            }
            (Some(k), None) => {
                for idx in range.start..range.end {
                    if let serde_json::Value::String(s) =
                        k.as_ref().read_value_as_json(idx as usize)?
                    {
                        map.insert(s, serde_json::Value::String("?".into()));
                    }
                }
            }
            (None, None) => {}
        }
        Ok(serde_json::Value::Object(map))
    }
}

/// Implementation of [`ReadValueAsJson`] for primitive [`ValueSequence`]s.
impl ReadValueAsJson for ValueSequence {
    fn read_value_as_json(&self, index: usize) -> Result<serde_json::Value> {
        verify_arg!(index, index < self.len());
        if self.presence.is_null(index) {
            return Ok(serde_json::Value::Null);
        }
        let signed = self.type_desc.signed;
        match self.type_desc.basic_type {
            BasicType::Unit => Ok(serde_json::Value::Null),

            BasicType::Boolean => {
                let values = self.as_slice::<u8>();
                Ok(serde_json::Value::Bool(values[index] != 0))
            }

            BasicType::Int8 => {
                if signed {
                    let values = self.as_slice::<i8>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                } else {
                    let values = self.as_slice::<u8>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                }
            }

            BasicType::Int16 => {
                if signed {
                    let values = self.as_slice::<i16>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                } else {
                    let values = self.as_slice::<u16>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                }
            }

            BasicType::Int32 => {
                if signed {
                    let values = self.as_slice::<i32>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                } else {
                    let values = self.as_slice::<u32>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                }
            }

            BasicType::Int64 => {
                if signed {
                    let values = self.as_slice::<i64>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                } else {
                    let values = self.as_slice::<u64>();
                    Ok(serde_json::Value::Number(serde_json::Number::from(
                        values[index],
                    )))
                }
            }

            BasicType::Float32 => {
                let values = self.as_slice::<f32>();
                let float_val = values[index];
                if float_val.is_finite() {
                    Ok(serde_json::Value::Number(
                        serde_json::Number::from_f64(float_val as f64)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    // Handle NaN, Infinity, etc. as null or string representation
                    Ok(serde_json::Value::String(float_val.to_string()))
                }
            }

            BasicType::Float64 => {
                let values = self.as_slice::<f64>();
                let float_val = values[index];
                if float_val.is_finite() {
                    Ok(serde_json::Value::Number(
                        serde_json::Number::from_f64(float_val)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    // Handle NaN, Infinity, etc. as null or string representation
                    Ok(serde_json::Value::String(float_val.to_string()))
                }
            }

            BasicType::String => {
                let string_val = self.string_at(index);
                Ok(serde_json::Value::String(string_val.to_string()))
            }

            BasicType::Binary | BasicType::FixedSizeBinary => {
                let binary_val = self.binary_at(index);
                Ok(serde_json::Value::String(bytes_to_string(binary_val)))
            }

            BasicType::Guid => {
                let guid_bytes = self.binary_at(index);
                if guid_bytes.len() == 16 {
                    // Format as standard GUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
                    let guid_str = format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        guid_bytes[0],
                        guid_bytes[1],
                        guid_bytes[2],
                        guid_bytes[3],
                        guid_bytes[4],
                        guid_bytes[5],
                        guid_bytes[6],
                        guid_bytes[7],
                        guid_bytes[8],
                        guid_bytes[9],
                        guid_bytes[10],
                        guid_bytes[11],
                        guid_bytes[12],
                        guid_bytes[13],
                        guid_bytes[14],
                        guid_bytes[15]
                    );
                    Ok(serde_json::Value::String(guid_str))
                } else {
                    Ok(serde_json::Value::String(bytes_to_string(guid_bytes)))
                }
            }

            BasicType::DateTime => {
                // DateTime is stored as i64 (ticks since epoch)
                let values = self.as_slice::<i64>();
                let ticks = values[index];
                // Convert to ISO 8601 string or store as number - for now use number
                Ok(serde_json::Value::Number(serde_json::Number::from(ticks)))
            }

            // Complex types should not be handled by ValueSequence directly
            BasicType::List
            | BasicType::FixedSizeList
            | BasicType::Struct
            | BasicType::Map
            | BasicType::Union => Err(Error::invalid_arg(
                "sequence type",
                format!(
                    "Complex type {} should not be handled by ValueSequence directly",
                    self.type_desc
                ),
            )),
        }
    }
}

/// Convert a byte slice into a printable `String`, escaping non-printable bytes as `\xHH`.
fn bytes_to_string(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len());
    for &byte in bytes {
        if (32..=126).contains(&byte) {
            result.push(byte as char);
        } else {
            result.push_str(&format!("\\x{byte:02x}"));
        }
    }
    result
}
