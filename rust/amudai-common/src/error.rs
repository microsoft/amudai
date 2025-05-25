use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(Box<ErrorKind>);

pub type StdErrorBoxed = Box<dyn std::error::Error + Send + Sync + 'static>;

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        self.0.as_ref()
    }

    pub fn into_kind(self) -> ErrorKind {
        *self.0
    }

    pub fn invalid_format(name: impl Into<String>) -> Error {
        Error(
            ErrorKind::InvalidFormat {
                element: name.into(),
                message: Default::default(),
            }
            .into(),
        )
    }

    pub fn invalid_arg(name: impl Into<String>, message: impl Into<String>) -> Error {
        Error(
            ErrorKind::InvalidArgument {
                name: name.into(),
                message: message.into(),
            }
            .into(),
        )
    }

    pub fn invalid_operation(name: impl Into<String>) -> Error {
        Error(ErrorKind::InvalidOperation { name: name.into() }.into())
    }

    pub fn not_implemented(message: impl Into<String>) -> Error {
        Error(
            ErrorKind::NotImplemented {
                message: message.into(),
            }
            .into(),
        )
    }

    pub fn io(context: impl Into<String>, source: std::io::Error) -> Error {
        Error(
            ErrorKind::Io {
                context: context.into(),
                source,
            }
            .into(),
        )
    }

    pub fn arrow<E>(context: impl Into<String>, source: E) -> Error
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error(
            ErrorKind::Arrow {
                context: context.into(),
                source: Box::new(source),
            }
            .into(),
        )
    }
}

#[derive(Debug, Error)]
pub enum ErrorKind {
    #[error("invalid argument {name}: {message}")]
    InvalidArgument { name: String, message: String },

    #[error("invalid operation {name}")]
    InvalidOperation { name: String },

    #[error("not yet implemented: {message}")]
    NotImplemented { message: String },

    #[error(
        "failed to resolve url '{url}' (relative: {}), reason: {reason}",
        relative.as_deref().unwrap_or_default())]
    ResolveUrl {
        url: String,
        relative: Option<String>,
        reason: String,
    },

    #[error("checksum mismatch for '{element}'")]
    ChecksumMismatch { element: String },

    #[error("invalid storage format for '{element}': {message}")]
    InvalidFormat { element: String, message: String },

    #[error("invalid FlatBuffers format for '{element}'")]
    InvalidFlatBuffer {
        element: String,
        source: planus::Error,
    },

    #[error("IO error for '{context}': {source}'")]
    Io {
        context: String,
        source: std::io::Error,
    },

    #[error("Arrow error: {context}")]
    Arrow {
        context: String,
        source: StdErrorBoxed,
    },

    #[error("destination buffer is too small")]
    DestBufferTooSmall,
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Error(kind.into())
    }
}

impl From<planus::Error> for Error {
    fn from(e: planus::Error) -> Self {
        ErrorKind::InvalidFlatBuffer {
            element: String::new(),
            source: e,
        }
        .into()
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::io("", e)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        Error::invalid_operation("conversion")
    }
}
