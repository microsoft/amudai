use std::time::Instant;

use amudai_sequence::values::Values;

fn main() {
    let t0 = Instant::now();
    test_values_push(1024 * 1024 * 1024 + std::env::args().count());
    let elapsed = t0.elapsed();
    println!("elapsed: {elapsed:?}");
}

#[inline(never)]
#[no_mangle]
fn test_values_push(count: usize) {
    let mut values = Values::with_capacity::<i64>(1024);
    let mut c = 0;
    while c < count {
        values.write::<i64, _>(1024, |s| {
            s.iter_mut().enumerate().for_each(|(i, x)| *x = i as i64);
        });
        c += values.len::<i64>();
        // values.clear();
    }
}
