// Timing macro: evaluates the expression once, prints elapsed time and returns the result.
#[macro_export]
macro_rules! time {
    ($expr:expr) => {{
        let __start = ::std::time::Instant::now();
        let __result = { $expr };
        let __elapsed = __start.elapsed();
        // Compute milliseconds with up to 3 fractional digits and trim trailing zeros
        let __ms = __elapsed.as_secs_f64() * 1000.0;
        let __ms_str = {
            let s = ::std::format!("{:.3}", __ms);
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        };
        println!("[time] {} took {} ms", stringify!($expr), __ms_str);
        std::hint::black_box(__result)
    }};
}
