use crate::wal::{Event, Record};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::VecDeque};

const SCALE: i128 = 1_000_000;
const WINDOW: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Target {
    Long,
    Short,
    #[default]
    Flat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Factors {
    pub book_ppm: i64,
    pub flow_ppm: i64,
    pub spread_bps_ppm: i64,
    pub score_ppm: i64,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Signal {
    pub seq: u64,
    pub logical_ns: u64,
    pub factors: Factors,
    pub target: Target,
    pub reason: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Outcome {
    pub seq: u64,
    pub logical_ns: u64,
    pub class: &'static str,
    pub state: &'static str,
    pub reason: &'static str,
    pub factors: Option<Factors>,
    pub target: Target,
    pub signal: Option<Signal>,
}

#[derive(Deserialize)]
#[serde(tag = "stream", content = "data")]
enum Input<'a> {
    #[serde(rename = "btcusdt@trade")]
    Trade(#[serde(borrow)] Trade<'a>),
    #[serde(rename = "btcusdt@bookTicker")]
    Quote(#[serde(borrow)] Quote<'a>),
}

#[derive(Deserialize)]
struct Trade<'a> {
    #[serde(borrow)]
    s: Cow<'a, str>,
    t: u64,
    #[serde(borrow)]
    p: Cow<'a, str>,
    #[serde(borrow)]
    q: Cow<'a, str>,
    m: bool,
}

#[derive(Deserialize)]
struct Quote<'a> {
    #[serde(borrow)]
    s: Cow<'a, str>,
    u: u64,
    #[serde(borrow)]
    b: Cow<'a, str>,
    #[serde(rename = "B", borrow)]
    bid_qty: Cow<'a, str>,
    #[serde(borrow)]
    a: Cow<'a, str>,
    #[serde(rename = "A", borrow)]
    ask_qty: Cow<'a, str>,
}

struct Book {
    time_ns: u64,
    bid: i128,
    ask: i128,
    bid_qty: i128,
    ask_qty: i128,
}

#[derive(Default)]
pub struct Engine {
    connected: bool,
    trade_id: Option<u64>,
    book_id: Option<u64>,
    book: Option<Book>,
    trades: VecDeque<(i128, bool)>,
    buy_qty: i128,
    sell_qty: i128,
    target: Target,
}

fn decimal(text: &str) -> Result<i128> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|x| x.is_ascii_digit()),
        "invalid decimal"
    );
    ensure!(
        fraction.len() <= 8 && fraction.bytes().all(|x| x.is_ascii_digit()),
        "invalid decimal precision"
    );
    let fraction_len = fraction.len() as u32;
    let whole: i64 = whole.parse()?;
    let fraction: i64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse()?
    };
    let scaled = whole
        .checked_mul(100_000_000)
        .and_then(|w| w.checked_add(fraction * 10_i64.pow(8 - fraction_len)))
        .ok_or_else(|| anyhow::anyhow!("decimal overflow"))?;
    Ok(scaled as i128)
}

impl Engine {
    fn clear_market(&mut self) {
        self.book = None;
        self.book_id = None;
        self.trades.clear();
        self.buy_qty = 0;
        self.sell_qty = 0;
    }

    fn market(&mut self, text: &str, time_ns: u64) -> Result<&'static str> {
        ensure!(self.connected, "market data while disconnected");
        match serde_json::from_str(text)? {
            Input::Trade(trade) => {
                let price = decimal(&trade.p)?;
                let qty = decimal(&trade.q)?;
                ensure!(
                    trade.s == "BTCUSDT" && price > 0 && qty > 0,
                    "invalid trade"
                );
                if self.trade_id.is_some_and(|last| trade.t <= last) {
                    return Ok("duplicate");
                }
                self.trade_id = Some(trade.t);
                // m=true means the buyer is maker, so the aggressor is selling.
                self.trades.push_back((qty, trade.m));
                if trade.m {
                    self.sell_qty += qty;
                } else {
                    self.buy_qty += qty;
                }
                if self.trades.len() > WINDOW {
                    let (qty, sell) = self.trades.pop_front().unwrap();
                    if sell {
                        self.sell_qty -= qty;
                    } else {
                        self.buy_qty -= qty;
                    }
                }
                Ok("trade")
            }
            Input::Quote(quote) => {
                let book = Book {
                    time_ns,
                    bid: decimal(&quote.b)?,
                    ask: decimal(&quote.a)?,
                    bid_qty: decimal(&quote.bid_qty)?,
                    ask_qty: decimal(&quote.ask_qty)?,
                };
                ensure!(
                    quote.s == "BTCUSDT"
                        && book.bid > 0
                        && book.ask >= book.bid
                        && book.bid_qty + book.ask_qty > 0,
                    "invalid book"
                );
                if self.book_id.is_some_and(|last| quote.u <= last) {
                    return Ok("duplicate");
                }
                self.book_id = Some(quote.u);
                self.book = Some(book);
                Ok("book")
            }
        }
    }

    pub fn on_event(&mut self, record: &Record) -> Outcome {
        let class = match &record.event {
            Event::Connected => {
                self.clear_market();
                self.connected = true;
                "control"
            }
            Event::Disconnected(_) | Event::Finished(_) => {
                self.connected = false;
                self.clear_market();
                "control"
            }
            Event::Message(text) => self.market(text, record.offset_ns).unwrap_or_else(|error| {
                tracing::warn!(seq = record.seq, %error, "invalid market event");
                "invalid"
            }),
        };
        let ready = self.connected && self.book.is_some() && self.trades.len() == WINDOW;
        let state = if !self.connected {
            "DISCONNECTED"
        } else if ready {
            "READY"
        } else {
            "WARMING_UP"
        };
        let factors = self
            .book
            .as_ref()
            .filter(|_| !self.trades.is_empty())
            .map(|book| {
                let book_ppm =
                    (book.bid_qty - book.ask_qty) * SCALE / (book.bid_qty + book.ask_qty);
                let flow_ppm =
                    (self.buy_qty - self.sell_qty) * SCALE / (self.buy_qty + self.sell_qty);
                Factors {
                    book_ppm: book_ppm as i64,
                    flow_ppm: flow_ppm as i64,
                    spread_bps_ppm: ((book.ask - book.bid) * 20_000 * SCALE / (book.ask + book.bid))
                        as i64,
                    score_ppm: ((book_ppm + flow_ppm) / 2) as i64,
                }
            });
        let reason = if class == "invalid" || class == "duplicate" {
            class
        } else if !ready {
            state
        } else {
            let book = self.book.as_ref().unwrap();
            if record.offset_ns.saturating_sub(book.time_ns) > 1_000_000_000 {
                "stale_quote"
            } else if (book.ask - book.bid) * 20_000 > 5 * (book.ask + book.bid) {
                "wide_spread"
            } else {
                "eligible"
            }
        };
        let mut signal = None;
        if reason == "eligible" {
            let factors = factors.as_ref().unwrap();
            let next = if factors.score_ppm >= 300_000 {
                Target::Long
            } else if factors.score_ppm <= -300_000 {
                Target::Short
            } else if factors.score_ppm.abs() <= 100_000 {
                Target::Flat
            } else {
                self.target
            };
            if next != self.target {
                self.target = next;
                signal = Some(Signal {
                    seq: record.seq,
                    logical_ns: record.offset_ns,
                    factors: factors.clone(),
                    target: next,
                    reason: "score_threshold",
                });
            }
        }
        Outcome {
            seq: record.seq,
            logical_ns: record.offset_ns,
            class,
            state,
            reason,
            factors,
            target: self.target,
            signal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(seq: u64, event: Event) -> Record {
        Record {
            seq,
            connection: 1,
            utc_ns: seq,
            offset_ns: seq,
            event,
        }
    }

    fn book(id: u64, bid_qty: u64, ask_qty: u64) -> Event {
        Event::Message(json!({"stream":"btcusdt@bookTicker", "data":{"s":"BTCUSDT","u":id,"b":"100","a":"100.01","B":bid_qty.to_string(),"A":ask_qty.to_string()}}).to_string())
    }

    fn trade(id: u64, sell: bool) -> Event {
        Event::Message(json!({"stream":"btcusdt@trade", "data":{"s":"BTCUSDT","t":id,"p":"100","q":"1","m":sell}}).to_string())
    }

    #[test]
    fn hand_calculated_window_dedup_signals_and_reconnect() {
        let mut engine = Engine::default();
        engine.on_event(&record(1, Event::Connected));
        engine.on_event(&record(2, book(1, 1, 1)));
        for id in 1..1000 {
            assert!(
                engine
                    .on_event(&record(id + 2, trade(id, false)))
                    .signal
                    .is_none()
            );
        }
        let long = engine.on_event(&record(1002, trade(1000, false)));
        assert_eq!(long.signal.unwrap().target, Target::Long);
        assert_eq!(long.factors.unwrap().flow_ppm, 1_000_000);
        assert_eq!(
            engine.on_event(&record(1003, trade(1000, true))).class,
            "duplicate"
        );
        let mut transitions = vec![];
        for id in 1001..=2000 {
            if let Some(signal) = engine.on_event(&record(id + 3, trade(id, true))).signal {
                transitions.push(signal.target);
            }
        }
        assert_eq!(transitions, vec![Target::Flat, Target::Short]);
        let before = engine.sell_qty;
        let invalid = Event::Message("{bad json".into());
        assert_eq!(engine.on_event(&record(2004, invalid)).class, "invalid");
        assert_eq!(engine.sell_qty, before);
        let disconnected = engine.on_event(&record(2005, Event::Disconnected("test".into())));
        assert_eq!(disconnected.state, "DISCONNECTED");
        assert_eq!(disconnected.target, Target::Short);
        assert!(disconnected.signal.is_none());
        engine.on_event(&record(2006, Event::Connected));
        engine.on_event(&record(2007, book(1, 1, 1)));
        assert_eq!(
            engine.on_event(&record(2008, trade(2000, true))).class,
            "duplicate"
        );
        assert_eq!(
            engine.on_event(&record(2009, trade(2001, false))).state,
            "WARMING_UP"
        );
    }

    #[test]
    fn decimal_precision_and_overflow() {
        assert_eq!(decimal("0.00000001").unwrap(), 1);
        assert_eq!(decimal("80939.17000000").unwrap(), 8_093_917_000_000);
        for invalid in ["-1", "+1", "1e8", "1.000000001", "NaN", "100000000000"] {
            assert!(decimal(invalid).is_err(), "accepted {invalid}");
        }
    }
}
