use serde_json::json;
use std::collections::VecDeque;
use yino::{
    engine::{Engine, Outcome, Target},
    wal::{Event, Record},
};

struct Fixture {
    engine: Engine,
    seq: u64,
}

impl Fixture {
    fn new() -> Self {
        let mut fixture = Self {
            engine: Engine::default(),
            seq: 0,
        };
        fixture.event(Event::Connected, 0);
        fixture
    }

    fn event(&mut self, event: Event, offset_ns: u64) -> Outcome {
        self.seq += 1;
        self.engine.on_event(&Record {
            seq: self.seq,
            connection: 1,
            utc_ns: self.seq,
            offset_ns,
            event,
        })
    }

    fn book(&mut self, id: u64, prices: (&str, &str), qty: (u64, u64), ns: u64) -> Outcome {
        self.event(
            Event::Message(
                json!({"stream":"btcusdt@bookTicker","data":{
                    "s":"BTCUSDT","u":id,"b":prices.0,"a":prices.1,
                    "B":qty.0.to_string(),"A":qty.1.to_string()
                }})
                .to_string(),
            ),
            ns,
        )
    }

    fn trade(&mut self, id: u64, qty: u64, sell: bool, ns: u64) -> Outcome {
        self.event(
            Event::Message(
                json!({"stream":"btcusdt@trade","data":{
                    "s":"BTCUSDT","t":id,"p":"100","q":qty.to_string(),"m":sell
                }})
                .to_string(),
            ),
            ns,
        )
    }

    fn warm(&mut self, sell: bool) {
        self.book(1, ("100", "100.01"), (1, 1), 0);
        for id in 1..=1000 {
            self.trade(id, 1, sell, id);
        }
    }
}

#[test]
fn freshness_and_spread_compare_exact_boundaries() {
    let mut fixture = Fixture::new();
    fixture.warm(false);
    assert_eq!(
        fixture.trade(1001, 1, false, 1_000_000_000).reason,
        "eligible"
    );
    let stale = fixture.trade(1002, 1, true, 1_000_000_001);
    assert_eq!(stale.reason, "stale_quote");
    assert!(stale.signal.is_none());
    assert_eq!(stale.target, Target::Long);
    let exact = fixture.book(2, ("19995", "20005"), (1, 1), 1_000_000_002);
    assert_eq!(exact.reason, "eligible");
    assert_eq!(exact.factors.unwrap().spread_bps_ppm, 5_000_000);
    let outside = fixture.book(3, ("19995", "20005.00000001"), (1, 1), 1_000_000_003);
    // The rounded display value is still 5 bps; admission must use cross products.
    assert_eq!(outside.factors.unwrap().spread_bps_ppm, 5_000_000);
    assert_eq!(outside.reason, "wide_spread");
    assert!(outside.signal.is_none());
}

#[test]
fn target_thresholds_include_exact_boundary_and_hold_between_bands() {
    for sell in [false, true] {
        let mut fixture = Fixture::new();
        fixture.book(1, ("100", "100.01"), if sell { (1, 0) } else { (0, 1) }, 0);
        for id in 1..=1000 {
            fixture.trade(id, 1, sell, id);
        }
        let quantities = if sell {
            [(700001, 299999), (7, 3), (899999, 100001), (9, 1)]
        } else {
            [(299999, 700001), (3, 7), (100001, 899999), (1, 9)]
        };
        let direction = if sell { Target::Short } else { Target::Long };
        for (index, (qty, expected)) in quantities
            .into_iter()
            .zip([
                (Target::Flat, 299999),
                (direction, 300000),
                (direction, 100001),
                (Target::Flat, 100000),
            ])
            .enumerate()
        {
            let outcome = fixture.book(
                index as u64 + 2,
                ("100", "100.01"),
                qty,
                1001 + index as u64,
            );
            assert_eq!(
                outcome.factors.unwrap().score_ppm,
                if sell { -expected.1 } else { expected.1 }
            );
            assert_eq!(outcome.target, expected.0);
            assert_eq!(outcome.signal.is_some(), index == 1 || index == 3);
        }
    }
}

#[test]
fn unequal_volume_window_matches_full_rescan_and_deduplicates() {
    let mut fixture = Fixture::new();
    fixture.book(1, ("100", "100.01"), (3, 7), 0);
    let mut reference = VecDeque::new();
    for id in 1..=4000 {
        let qty = (id * 7919) % 997 + 1;
        let sell = id % 7 < 3;
        let outcome = fixture.trade(id, qty, sell, id);
        reference.push_back((qty as i128, sell));
        if reference.len() > 1000 {
            reference.pop_front();
        }
        // Recompute the full window rather than mirroring the incremental totals.
        let buy: i128 = reference.iter().filter(|(_, s)| !s).map(|(q, _)| q).sum();
        let sold: i128 = reference.iter().filter(|(_, s)| *s).map(|(q, _)| q).sum();
        let flow = (buy - sold) * 1_000_000 / (buy + sold);
        let factors = outcome.factors.unwrap();
        assert_eq!(factors.flow_ppm, flow as i64, "trade {id}");
        assert_eq!(factors.book_ppm, -400_000);
        assert_eq!(factors.score_ppm, ((flow - 400_000) / 2) as i64);
        assert_eq!(
            outcome.state,
            if id < 1000 { "WARMING_UP" } else { "READY" }
        );
        if id % 101 == 0 {
            let duplicate = fixture.trade(id, qty * 2, !sell, id);
            assert_eq!(duplicate.class, "duplicate");
            assert_eq!(duplicate.factors.unwrap(), factors);
            assert!(duplicate.signal.is_none());
        }
    }
}

#[test]
fn typed_input_accepts_reordered_fields_and_escaped_strings() {
    let mut fixture = Fixture::new();
    let outcome = fixture.event(Event::Message(
        r#"{"data":{"s":"\u0042TCUSDT","t":1,"p":"1\u0030\u0030","q":"1","m":false,"unused":{"nested":[1,2,3]}},"stream":"btcusdt@trade"}"#.into()
    ), 1);
    assert_eq!(outcome.class, "trade");
    let invalid = fixture.event(Event::Message(
        r#"{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","t":2,"p":"100","q":"-1","m":false}}"#.into()
    ), 2);
    assert_eq!(invalid.class, "invalid");
    // Invalid values must not advance the trade watermark.
    assert_eq!(fixture.trade(2, 1, false, 3).class, "trade");
}
