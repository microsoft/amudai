use std::ops::Range;

use amudai_ranges::RangeListSlice;

use crate::{
    PositionSet,
    segment::{Segment, builder::SegmentBuilder},
};

pub struct PositionSetBuilder {
    builder: SegmentBuilder,
    segments: Vec<Segment>,
}

impl Default for PositionSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionSetBuilder {
    pub fn new() -> PositionSetBuilder {
        PositionSetBuilder {
            builder: SegmentBuilder::new(0..Segment::SPAN),
            segments: Vec::new(),
        }
    }

    #[inline]
    pub fn next_pos(&self) -> u64 {
        self.builder.min_next_pos()
    }

    #[inline]
    pub fn push_position(&mut self, pos: u64) {
        while !self.builder.push_position(pos) {
            self.flush_segment();
        }
    }

    pub fn push_range(&mut self, mut range: Range<u64>) {
        loop {
            range = self.builder.push_range(range);
            if !range.is_empty() {
                self.flush_segment();
            } else {
                break;
            }
        }
    }

    pub fn extend_from_position_slice(&mut self, mut positions: &[u64]) {
        loop {
            positions = self.builder.extend_from_position_slice(positions);
            if !positions.is_empty() {
                self.flush_segment();
            } else {
                break;
            }
        }
    }

    pub fn extend_from_range_slice(&mut self, ranges: &[Range<u64>]) {
        let mut ranges = RangeListSlice::new(ranges);
        loop {
            ranges = self.builder.extend_from_range_slice(ranges);
            if !ranges.is_empty() {
                self.flush_segment();
            } else {
                break;
            }
        }
    }

    pub fn build(mut self, span: u64) -> PositionSet {
        self.flush_up_to(span);
        self.flush_last_segment(span);
        PositionSet::new(self.segments, span)
    }
}

impl PositionSetBuilder {
    fn flush_up_to(&mut self, pos: u64) {
        while self.builder.span().end <= pos {
            self.flush_segment();
        }
    }

    fn flush_segment(&mut self) {
        let segment = self.builder.build(None);
        let segment_end = segment.span().end;
        self.builder.reset(segment_end..segment_end + Segment::SPAN);
        self.segments.push(segment);
    }

    fn flush_last_segment(&mut self, span_end: u64) {
        if span_end == self.builder.span().start {
            return;
        }
        let segment = self.builder.build(Some(span_end));
        self.segments.push(segment);
    }
}
