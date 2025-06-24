use super::{AnalysisOutcome, BinaryValuesSequence, EncodingContext};
use crate::encodings::{EncodingConfig, EncodingPlan, NullMask, numeric::value::ValueWriter};
use amudai_bytes::buffer::AlignedByteVec;
use amudai_common::error::Error;
use amudai_format::schema::BasicTypeDescriptor;
use amudai_sequence::{
    offsets::Offsets, presence::Presence, sequence::ValueSequence, values::Values,
};

pub(crate) fn analyze_offsets(
    values: &BinaryValuesSequence,
    config: &EncodingConfig,
    context: &EncodingContext,
) -> amudai_common::Result<(Option<AnalysisOutcome>, usize)> {
    if values.fixed_value_size().is_some() {
        Ok((None, 0))
    } else if let Some(offsets) = values.offsets() {
        if let Some(outcome) = context.numeric_encoders.get::<u64>().analyze(
            &offsets,
            &NullMask::None,
            &config.make_cascading_config(),
            context,
        )? {
            let encoded_size = outcome.encoded_size;
            Ok((Some(outcome), encoded_size))
        } else {
            Ok((None, offsets.len() * std::mem::size_of::<u64>()))
        }
    } else {
        Err(Error::invalid_arg(
            "values",
            "Unsupported sequence: either fixed size or offsets must be present",
        ))
    }
}

/// Encodes binary values offsets.
/// The method expects that the target buffer is aligned to `ALIGNMENT_BYTES`.
pub(crate) fn encode_offsets(
    values: &BinaryValuesSequence,
    target: &mut AlignedByteVec,
    plan: Option<&EncodingPlan>,
    context: &EncodingContext,
) -> amudai_common::Result<(u8, bool, usize)> {
    let initial_size = target.len();

    let mut offset_width = 0u8;
    let mut offsets_cascading = false;

    if values.fixed_value_size().is_some() {
        // Zero offset width indicates fixed size values.
    } else if let Some(offsets) = values.offsets() {
        offset_width = 8;
        if let Some(offsets_plan) = plan {
            context.numeric_encoders.get::<u64>().encode(
                &offsets,
                &NullMask::None,
                target,
                offsets_plan,
                context,
            )?;
            offsets_cascading = true;
        } else {
            target.write_values(&offsets);
        }
    } else {
        return Err(Error::invalid_arg(
            "values",
            "Unsupported sequence: either fixed size or offsets must be present",
        ));
    }

    Ok((offset_width, offsets_cascading, target.len() - initial_size))
}

/// Decodes binary values offsets and creates an Amudai sequence.
/// The method expects that the buffer is aligned to `ALIGNMENT_BYTES`.
pub(crate) fn decode_offsets_and_make_sequence(
    buffer: &[u8],
    values: AlignedByteVec,
    presence: Presence,
    type_desc: BasicTypeDescriptor,
    offset_width: u8,
    offsets_cascading: bool,
    context: &EncodingContext,
) -> amudai_common::Result<ValueSequence> {
    let values = Values::from_vec(values);
    let offsets = if offset_width == 0 {
        // Zero offset width indicates fixed size values.
        // For fixed size values, we don't need to return offsets.
        // Consumers should be able to calculate offsets based on the fixed size.
        None
    } else if offset_width == 8 {
        let mut offsets = AlignedByteVec::new();
        if offsets_cascading {
            context.numeric_encoders.get::<u64>().decode(
                buffer,
                presence.len() + 1,
                None,
                &mut offsets,
                context,
            )?;
        } else {
            if buffer.len() % 8 != 0 {
                return Err(Error::invalid_format("Invalid encoded offsets size"));
            }
            offsets.extend_from_slice(buffer);
        }
        Some(Offsets::from_values(Values::from_vec(offsets)))
    } else {
        return Err(Error::invalid_format("Invalid offset width"));
    };

    Ok(ValueSequence {
        values,
        offsets,
        presence,
        type_desc,
    })
}

/// Inspects the offsets in the encoded buffer and returns an encoding plan
/// that was used to encode them.
pub(crate) fn inspect_offsets(
    buffer: &[u8],
    offset_width: u8,
    offsets_cascading: bool,
    context: &EncodingContext,
) -> amudai_common::Result<Option<EncodingPlan>> {
    if offset_width == 0 {
        Ok(None)
    } else if offset_width == 8 {
        if offsets_cascading {
            Ok(Some(
                context
                    .numeric_encoders
                    .get::<u64>()
                    .inspect(buffer, context)?,
            ))
        } else {
            Ok(None)
        }
    } else {
        Err(Error::invalid_format("Invalid offset width"))
    }
}
