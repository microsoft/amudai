/// Macro for handling `Result<T, E>` in functions that return `Option<Result<T, E>>`.
///
/// - If `expr` evaluates to `Ok(t)`, the macro yields `t`.
/// - If `expr` evaluates to `Err(e)`, the macro causes the enclosing function to
///   return `Some(Err(e))`.
///
/// This is especially useful inside a `next()` implementation of an
/// `Iterator<Item = Result<T, E>>` when calling helper functions that return
/// `Result<T, E>`.
///
/// The enclosing function *must* have a return type compatible with
/// `Option<Result<_, E>>`.
#[macro_export]
macro_rules! try_or_ret_some_err {
    ($expr:expr) => {
        match $expr {
            Ok(value) => value,
            Err(err) => {
                return Some(Err(err));
            }
        }
    };
}
