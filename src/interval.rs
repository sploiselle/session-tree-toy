use abomonation::Abomonation;
use chrono::NaiveDateTime;

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ndt(pub NaiveDateTime);
impl Abomonation for Ndt {}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Interval {
    pub start: Ndt,
    pub end: Ndt,
}

impl Interval {
    pub fn parse(i: (&str, &str)) -> Interval {
        let start = NaiveDateTime::parse_from_str(i.0, "%Y-%m-%d %H:%M:%S").unwrap();
        let end = NaiveDateTime::parse_from_str(i.1, "%Y-%m-%d %H:%M:%S").unwrap();
        Self::new(start, end)
    }

    pub fn new(start: NaiveDateTime, end: NaiveDateTime) -> Interval {
        Interval {
            start: Ndt(start),
            end: Ndt(end),
        }
    }

    fn merge(&mut self, i: Interval) {
        self.start = std::cmp::min(self.start, i.start);
        self.end = std::cmp::max(self.end, i.end);
    }

    fn interval_cmp(&self, other: &Interval) -> IntervalCmp {
        if other.start < self.start && other.end < self.start {
            IntervalCmp::LExceeds
        } else if self.end < other.start && self.end < other.end {
            IntervalCmp::RExceeds
        } else {
            IntervalCmp::Overlaps
        }
    }
}

impl Abomonation for Interval {}

#[derive(Debug, Clone, Hash)]
enum IntervalCmp {
    LExceeds,
    Overlaps,
    RExceeds,
}

#[derive(Debug, Clone, Hash)]
pub struct Sessions(pub Vec<Interval>);

impl Sessions {
    pub fn insert_interval(&mut self, i: Interval) {
        if self.0.is_empty() {
            self.0.push(i);
            return;
        }

        let [start_in, end_in] = self.find_pos(&i);
        if start_in != end_in {
            let mut e = self.0.remove(end_in);
            e.merge(i);
            self.0[start_in].merge(e);
        } else {
            match self.0[start_in].interval_cmp(&i) {
                IntervalCmp::LExceeds => self.0.insert(start_in, i),
                IntervalCmp::Overlaps => self.0[start_in].merge(i),
                IntervalCmp::RExceeds => {
                    if start_in == self.0.len() - 1 {
                        self.0.push(i)
                    } else {
                        self.0.insert(start_in + 1, i)
                    }
                }
            }
        }
    }

    pub fn find_session(&self, d: Ndt) -> Option<Interval> {
        if self.0.is_empty() {
            return None;
        }

        Some(self.0[self.find_within(&d, 0, self.0.len())].clone())
    }

    fn find_pos(&self, i: &Interval) -> [usize; 2] {
        let s = self.find_within(&i.start, 0, self.0.len());
        let e = self.find_within(&i.end, 0, self.0.len());
        [s, e]
    }

    fn find_within(&self, d: &Ndt, l: usize, r: usize) -> usize {
        if r - l == 1 {
            return l;
        }

        let m = (l + r) / 2;
        let el = &self.0[m];
        if &el.start <= d && d <= &el.end {
            m
        } else if d < &el.start {
            self.find_within(d, l, m)
        } else {
            self.find_within(d, m, r)
        }
    }
}
