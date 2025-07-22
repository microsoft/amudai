use super::{
    AnalysisOutcome, EncodingContext, EncodingKind, NumericEncoding,
    stats::{NumericStats, NumericStatsCollectorFlags},
    value::{NumericValue, ValueWriter},
};
use crate::encodings::{EncodingConfig, EncodingParameters, EncodingPlan, NullMask};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;

/// Plain uncompressed encoding.
pub struct PlainEncoding<T>(std::marker::PhantomData<T>);

impl<T> PlainEncoding<T>
where
    T: NumericValue,
{
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NumericEncoding<T> for PlainEncoding<T>
where
    T: NumericValue,
{
    fn kind(&self) -> EncodingKind {
        EncodingKind::Plain
    }

    fn is_suitable(&self, _config: &EncodingConfig, _stats: &NumericStats<T>) -> bool {
        // The encoding is called explicitly.
        false
    }

    fn stats_collector_flags(&self) -> NumericStatsCollectorFlags {
        NumericStatsCollectorFlags::empty()
    }

    fn analyze(
        &self,
        _values: &[T],
        _null_mask: &NullMask,
        _config: &EncodingConfig,
        _stats: &NumericStats<T>,
        _context: &EncodingContext,
    ) -> amudai_common::Result<Option<AnalysisOutcome>> {
        unimplemented!(
            "PlainEncoding::analyze is not implemented as this encoding is used explicitly."
        )
    }

    fn encode(
        &self,
        values: &[T],
        _null_mask: &NullMask,
        target: &mut AlignedByteVec,
        _plan: &EncodingPlan,
        _context: &EncodingContext,
    ) -> amudai_common::Result<usize> {
        let initial_size = target.len();
        target.write_value::<u16>(self.kind() as u16);
        target.write_values(values);
        Ok(target.len() - initial_size)
    }

    fn decode(
        &self,
        buffer: &[u8],
        value_count: usize,
        _params: Option<&EncodingParameters>,
        target: &mut AlignedByteVec,
        _context: &EncodingContext,
    ) -> amudai_common::Result<()> {
        if buffer.len() != value_count * T::SIZE {
            return Err(Error::invalid_format("Encoded buffer size is invalid"));
        }
        target.extend_from_slice(buffer);
        Ok(())
    }

    fn inspect(
        &self,
        _buffer: &[u8],
        _context: &EncodingContext,
    ) -> amudai_common::Result<EncodingPlan> {
        Ok(EncodingPlan {
            encoding: self.kind(),
            parameters: Default::default(),
            cascading_encodings: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::encodings::{EncodingKind, EncodingPlan, NullMask, numeric::EncodingContext};
    use amudai_bytes::buffer::AlignedByteVec;

    #[test]
    fn test_round_trip() {
        let context = EncodingContext::new();
        let data: Vec<i64> = (0..65536).map(|_| fastrand::i64(-1000..1000)).collect();

        let mut encoded = AlignedByteVec::new();
        let plan = EncodingPlan {
            encoding: EncodingKind::Plain,
            parameters: Default::default(),
            cascading_encodings: vec![],
        };
        context
            .numeric_encoders
            .get::<i64>()
            .encode(&data, &NullMask::None, &mut encoded, &plan, &context)
            .unwrap();

        let mut decoded = AlignedByteVec::new();
        context
            .numeric_encoders
            .get::<i64>()
            .decode(&encoded, data.len(), None, &mut decoded, &context)
            .unwrap();
        for (a, b) in data.iter().zip(decoded.typed_data::<i64>()) {
            assert_eq!(a, b);
        }
    }
}
