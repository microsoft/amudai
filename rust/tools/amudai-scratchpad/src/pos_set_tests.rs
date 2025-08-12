use amudai_position_set::{PositionSet, position_set_builder::PositionSetBuilder};
use anyhow::Result;

use crate::time;

pub fn set_ops(_args: &[Option<&str>]) -> Result<()> {
    dbg!(amudai_workflow::eager_pool::EagerPool::global().thread_count());

    let set = time!(test_build_from_positions(1000000000, 50000000));
    println!("{}", set.count_positions());
    let set1 = time!(test_build_from_slices(1000000000, 50000000));
    println!("{}", set.count_positions());

    let u_set = time!(set1.union(&set));
    println!("{}", u_set.count_positions());
    println!("{:?}", u_set.compute_stats());

    let i_set = time!(set1.intersect(&set));
    println!("{}", i_set.count_positions());
    println!("{:?}", i_set.compute_stats());

    drop(u_set);
    drop(i_set);

    let u_set = time!(set1.par_union(&set));
    println!("{}", u_set.count_positions());
    println!("{:?}", u_set.compute_stats());

    let i_set = time!(set1.par_intersect(&set));
    println!("{}", i_set.count_positions());
    println!("{:?}", i_set.compute_stats());

    // let set = time!(test_build_as_bits(10000000000, 500000000));
    // println!("{}", set.count_positions());
    // println!("{:?}", set.compute_stats());
    Ok(())
}

fn test_build_from_positions(span: u64, count: u64) -> PositionSet {
    let step = (span - 100) / count;
    let mut pos = 10;
    let mut builder = PositionSetBuilder::new();
    for _ in 0u64..count {
        builder.push_position(pos);
        pos += step;
    }
    builder.build(span)
}

fn test_build_from_slices(span: u64, count: u64) -> PositionSet {
    let step = (span - 10000) / count;
    let mut pos = 10; // 12
    let mut builder = PositionSetBuilder::new();
    let mut slice = [0u64; 256];
    while pos < span - 10000 {
        slice
            .iter_mut()
            .enumerate()
            .for_each(|(i, p)| *p = pos + (i as u64 + 1) * step);
        pos = *slice.last().unwrap() + step;

        builder.extend_from_position_slice(&slice);
    }
    builder.build(span)
}

#[allow(dead_code)]
fn test_build_as_bits(span: u64, count: u64) -> PositionSet {
    let step = (span - 100) / count;
    let mut pos = 10;
    let mut set = PositionSet::empty(span);
    for _ in 0u64..count {
        set.set(pos);
        pos += step;
    }
    // set.optimize();
    set
}
