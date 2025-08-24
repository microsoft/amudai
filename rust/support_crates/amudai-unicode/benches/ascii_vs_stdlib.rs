use amudai_unicode::ascii_check::IsAsciiFast;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_vs_stdlib(c: &mut Criterion) {
    let mut group = c.benchmark_group("is_ascii_fast_vs_stdlib");

    let sizes = [10, 100, 200, 1000, 10000];

    for &size in &sizes {
        group.throughput(Throughput::Bytes(size as u64));

        // Pure ASCII test data
        let ascii_data = vec![b'A'; size];

        group.bench_with_input(
            BenchmarkId::new("our_is_ascii_fast", size),
            &ascii_data,
            |b, data| {
                b.iter(|| {
                    let result = black_box(data.as_slice()).is_ascii_fast();
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("stdlib_is_ascii", size),
            &ascii_data,
            |b, data| {
                b.iter(|| {
                    let result = black_box(data.as_slice()).is_ascii();
                    black_box(result)
                });
            },
        );

        // Mixed data test
        let mut mixed_data = vec![b'A'; size];
        for i in (0..size).step_by(10) {
            if i < mixed_data.len() {
                mixed_data[i] = 0x80;
            }
        }

        group.bench_with_input(
            BenchmarkId::new("our_is_ascii_fast_mixed", size),
            &mixed_data,
            |b, data| {
                b.iter(|| {
                    let result = black_box(data.as_slice()).is_ascii_fast();
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("stdlib_is_ascii_mixed", size),
            &mixed_data,
            |b, data| {
                b.iter(|| {
                    let result = black_box(data.as_slice()).is_ascii();
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_vs_stdlib);
criterion_main!(benches);
