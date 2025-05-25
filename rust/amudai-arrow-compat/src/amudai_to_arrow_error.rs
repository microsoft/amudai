//! Utilities for converting amudai errors to Arrow-compatible errors.
//!
//! This module provides traits and implementations to convert errors from the
//! `amudai_common` crate into [`arrow_schema::ArrowError`] types. This simplifies
//! interoperability between amudai-based code and Arrow-based APIs.

use amudai_common::error::Error as AmudaiError;
use arrow_schema::ArrowError;

/// Trait for converting types into an [`ArrowError`].
///
/// This is useful for adapting errors from the `amudai` ecosystem
/// into Arrow's error handling conventions.
pub trait ToArrowError {
    /// Converts `self` into an [`ArrowError`].
    fn to_arrow_err(self) -> ArrowError;
}

/// Trait for converting results with custom errors into Arrow-compatible results.
///
/// This trait allows easy conversion of `Result<T, E>` where `E` implements
/// [`ToArrowError`] into `Result<T, ArrowError>`.
pub trait ToArrowResult {
    /// The success type of the result.
    type Success;

    /// Converts `self` into a `Result<Self::Success, ArrowError>`.
    fn to_arrow_res(self) -> Result<Self::Success, ArrowError>;
}

impl<T, E> ToArrowResult for Result<T, E>
where
    E: ToArrowError,
{
    type Success = T;

    /// Converts a `Result<T, E>` into a `Result<T, ArrowError>`,
    /// mapping the error using [`ToArrowError`].
    fn to_arrow_res(self) -> Result<Self::Success, ArrowError> {
        self.map_err(|e| e.to_arrow_err())
    }
}

impl ToArrowError for AmudaiError {
    /// Converts an [`AmudaiError`] into an [`ArrowError`].
    ///
    /// Maps specific `amudai_common::error::ErrorKind` variants to
    /// corresponding `ArrowError` variants, and falls back to
    /// `ArrowError::ExternalError` for other cases.
    fn to_arrow_err(self) -> ArrowError {
        use amudai_common::error::ErrorKind;
        match self.kind() {
            ErrorKind::InvalidArgument { name, message } => {
                ArrowError::InvalidArgumentError(format!("{name}: {message}"))
            }
            ErrorKind::NotImplemented { message } => {
                ArrowError::NotYetImplemented(format!("{message} (from amudai)"))
            }
            _ => ArrowError::ExternalError(self.into()),
        }
    }
}
