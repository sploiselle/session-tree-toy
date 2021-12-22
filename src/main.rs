use std::cell::RefCell;
use std::rc::Rc;

extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Consolidate, Iterate, Join};

mod interval;
use interval::{Interval, Sessions};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();

        worker.dataflow(|scope| {
            let intervals = input.to_collection(scope);

            // The structure where intervals are inserted and sessions are formed/merged.
            let sessions = Rc::new(RefCell::new(Sessions(vec![])));
            // The structure where each timestamp checks for its current session.
            let sessions_2 = sessions.clone();

            intervals
                .map(move |ts: Interval| {
                    sessions.borrow_mut().insert_interval(ts.clone());
                    (ts.start, sessions.borrow().find_session(ts.start).unwrap())
                })
                .inspect(|x| println!("begin {:?}", x))
                .iterate(move |intervals| {
                    intervals
                        .map(move |(ts, _interval)| {
                            (ts, sessions_2.borrow().find_session(ts.clone()).unwrap())
                        })
                        .inspect(|x| println!("intrm {:?}", x))
                        .consolidate()
                })
                .inspect(|x| println!("final {:?}", x));
        });

        input.advance_to(0);

        for (i, interval) in (&[
            ("2000-01-01 01:01:00", "2000-01-01 01:06:00"),
            ("2000-01-01 01:15:00", "2000-01-01 01:20:00"),
            ("2000-01-01 01:07:00", "2000-01-01 01:12:00"),
            ("2000-01-01 01:06:00", "2000-01-01 01:11:00"),
            ("2000-01-01 01:12:00", "2000-01-01 01:17:00"),
        ])
            .iter()
            .enumerate()
        {
            let interval = Interval::parse(*interval);
            input.insert(interval);
            input.advance_to(i + 1);
        }

        for (i, interval) in (&[("2000-01-01 01:07:00", "2000-01-01 01:12:00")])
            .iter()
            .enumerate()
        {
            let interval = Interval::parse(*interval);
            input.remove(interval);
            input.advance_to(i + 5);
        }
    })
    .expect("Computation terminated abnormally");
}
