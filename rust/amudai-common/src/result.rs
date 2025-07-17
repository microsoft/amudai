pub type Result<T> = std::result::Result<T, crate::error::Error>;

#[macro_export]
macro_rules! verify_arg {
    ($name:expr, $expr:expr) => {{
        let result = $expr;
        $crate::result::verify_arg(result, stringify!($name), stringify!($expr))?;
    }};
}

#[macro_export]
macro_rules! verify_data {
    ($name:expr, $expr:expr) => {{
        let result = $expr;
        $crate::result::verify_data(result, stringify!($name), stringify!($expr))?;
    }};
}

#[inline]
pub fn verify_arg(predicate: bool, name: &str, condition: &str) -> Result<()> {
    if predicate {
        Ok(())
    } else {
        invalid_arg(name, condition)
    }
}

#[inline]
pub fn verify_data(predicate: bool, name: &str, condition: &str) -> Result<()> {
    if predicate {
        Ok(())
    } else {
        invalid_format(name, condition)
    }
}

#[cold]
pub fn invalid_arg(name: &str, condition: &str) -> Result<()> {
    Err(crate::error::ErrorKind::InvalidArgument {
        name: name.to_string(),
        message: condition.to_string(),
    }
    .into())
}

#[cold]
pub fn invalid_format(name: &str, condition: &str) -> Result<()> {
    Err(crate::error::ErrorKind::InvalidFormat {
        element: name.to_string(),
        message: condition.to_string(),
    }
    .into())
}
