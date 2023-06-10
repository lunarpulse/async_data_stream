use async_std::channel;
use async_std::stream;
use async_std::stream::StreamExt;
use async_trait::async_trait;
use chrono::prelude::*;
use clap::Parser;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::iter::Iterator;
use std::time::Duration;
use xactor::*;
use yahoo_finance_api as yahoo;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
    #[clap(short, long)]
    interval: String,
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait AsyncStockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

///
/// Calculates the absolute and relative difference between the beginning and ending of an f64 series.
/// The relative difference is relative to the beginning.
///
struct PriceDifference {}

#[async_trait]
impl AsyncStockSignal for PriceDifference {
    ///
    /// A tuple `(absolute, relative)` to represent a price difference.
    ///
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

///
/// Window function to create a simple moving average
///
struct WindowedSMA {
    pub window_size: usize,
}

#[async_trait]
impl AsyncStockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

///
/// Find the maximum in a series of f64
///
struct MaxPrice {}

#[async_trait]
impl AsyncStockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

///
/// Find the maximum in a series of f64
///
struct MinPrice {}

#[async_trait]
impl AsyncStockSignal for MinPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

#[message]
#[derive(Debug, Clone)]
struct QuoteRequest {
    symbol: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

struct QuoteRequester {
    quote_processor_addr: Addr<QuoteProcessor>,
}

#[async_trait::async_trait]
impl Handler<QuoteRequest> for QuoteRequester {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: QuoteRequest) {
        let days = msg
            .to
            .naive_utc()
            .signed_duration_since(msg.from.naive_utc())
            .num_days() as usize;
        let provider = yahoo::YahooConnector::new();
        let data = match provider
            .get_quote_history(&msg.symbol, msg.from, msg.to)
            .await
        {
            Ok(response) => {
                if let Ok(quotes) = response.quotes() {
                    Quotes {
                        days,
                        symbol: msg.symbol,
                        timestamp: msg.from,
                        quotes,
                    }
                } else {
                    Quotes {
                        days,
                        symbol: msg.symbol,
                        timestamp: msg.from,
                        quotes: vec![],
                    }
                }
            }
            Err(e) => {
                eprintln!("Ignoring API error for symbol '{}': {}", msg.symbol, e);

                Quotes {
                    days,
                    symbol: msg.symbol,
                    timestamp: msg.from,
                    quotes: vec![],
                }
            }
        };
        if let Err(e) = self.quote_processor_addr.send(data) {
            eprint!("{}", e);
        }
    }
}

#[async_trait::async_trait]
impl Actor for QuoteRequester {
    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<RegisteredSignal> for QuoteRequester {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: RegisteredSignal) {
        println!("QuoteRequester :: RegisteredSignal recieved");
        ctx.stop(None)
    }
}

#[message]
#[derive(Debug, Clone)]
struct Quotes {
    days: usize,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub quotes: Vec<yahoo::Quote>,
}

struct QuoteProcessor {
    data_persistor_addr: Addr<DataPersistor>,
}

#[async_trait::async_trait]
impl Handler<Quotes> for QuoteProcessor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, mut msg: Quotes) {
        let data = if !msg.quotes.is_empty() {
            msg.quotes.sort_by_cached_key(|k| k.timestamp);
            msg.quotes.iter().map(|q| q.adjclose).collect()
        } else {
            vec![]
        };

        if !data.is_empty() {
            let diff = PriceDifference {};
            let min = MinPrice {};
            let max = MaxPrice {};
            let sma = WindowedSMA {
                window_size: msg.days / 2,
            };

            let period_max: f64 = max.calculate(&data).await.expect("none max");
            let period_min: f64 = min.calculate(&data).await.expect("no min");

            let last_price = *data.last().expect("no last");
            let (_, pct_change) = diff.calculate(&data).await.expect("no diff");
            let sma = sma.calculate(&data).await.expect("no sma");

            // a simple way to output CSV data
            println!(
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                msg.timestamp.to_rfc3339(),
                msg.symbol,
                last_price,
                pct_change * 100.0,
                period_min,
                period_max,
                sma.first().unwrap()
            );
            let data = FileFormat {
                symbol: msg.symbol,
                timestamp: msg.timestamp,
                price: last_price,
                pct_change,
                period_min,
                period_max,
                last_sma: *sma.first().unwrap(),
            };
            if let Err(e) = self.data_persistor_addr.send(data) {
                eprint!("{}", e);
            }
        }
    }
}

#[async_trait::async_trait]
impl Actor for QuoteProcessor {
    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<RegisteredSignal> for QuoteProcessor {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: RegisteredSignal) {
        println!("QuoteProcessor :: RegisteredSignal recieved");
        ctx.stop(None)
    }
}

#[message]
#[derive(Clone, Debug)]
struct FileFormat {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub pct_change: f64,
    pub period_min: f64,
    pub period_max: f64,
    pub last_sma: f64,
}
struct DataPersistor {
    pub filename: String,
    pub writer: Option<BufWriter<File>>,
    count: i32,
    days: usize,
}
impl DataPersistor {
    fn new(filename: String, days: usize) -> Self {
        DataPersistor {
            filename,
            writer: None,
            count: 0,
            days,
        }
    }
}
#[async_trait::async_trait]
impl Handler<FileFormat> for DataPersistor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: FileFormat) {
        if let Some(file) = &mut self.writer {
            let _ = writeln!(
                file,
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                msg.timestamp.to_rfc3339(),
                msg.symbol,
                msg.price,
                msg.pct_change * 100.0,
                msg.period_min,
                msg.period_max,
                msg.last_sma
            );
            if self.count % 23 == 0 {
                let _ = file.flush();
            }
            self.count += 1;
        } else {
            return;
        }
    }
}

#[async_trait::async_trait]
impl Handler<RegisteredSignal> for DataPersistor {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: RegisteredSignal) {
        println!("DataPersistor:: RegisteredSignal recieved");
        ctx.stop(None)
    }
}

#[async_trait::async_trait]
impl Actor for DataPersistor {
    async fn started(&mut self, _ctx: &mut Context<Self>) -> Result<()> {
        let mut file = File::create(&self.filename)
            .unwrap_or_else(|_| panic!("Could not open file '{}'", self.filename));
        let _ = writeln!(
            &mut file,
            "period start,symbol,price,change %,min,max,{}d avg",
            self.days
        );
        self.writer = Some(BufWriter::new(file));
        Ok(())
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        if let Some(writer) = &mut self.writer {
            writer.flush().expect("unable to flush")
        };
        ctx.stop(None);
    }
}

struct SignalHandler {
    signal_rx: channel::Receiver<()>,
    data_persistor_addr: Addr<DataPersistor>,
    quote_processor_addr: Addr<QuoteProcessor>,
    quote_requester_addr: Addr<QuoteRequester>,
}

impl SignalHandler {
    fn new(
        data_persistor_addr: Addr<DataPersistor>,
        quote_processor_addr: Addr<QuoteProcessor>,
        quote_requester_addr: Addr<QuoteRequester>,
    ) -> Self {
        let (signal_tx, signal_rx) = channel::bounded(100);

        println!("waiting for Signal Ctrl+C .");
        // Move the sender part into the ctrl-c handler
        async_std::task::spawn(async move {
            ctrlc::set_handler(move || {
                signal_tx.try_send(()).unwrap_or_else(|_| {
                    println!("Failed to send Ctrl+C event.");
                });
            })
            .expect("Error setting Ctrl+C handler");
        });
        SignalHandler {
            signal_rx,
            data_persistor_addr,
            quote_processor_addr,
            quote_requester_addr,
        }
    }
}

#[async_trait::async_trait]
impl Actor for SignalHandler {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        // Forward the signal Rx stream to the Actor's message queue
        let _ = self.signal_rx.recv().await.unwrap();

        ctx.send_later(RegisteredSignal, Duration::from_millis(1));
        if let Err(e) = self.data_persistor_addr.send(RegisteredSignal) {
            eprintln!("{}", e)
        }
        println!("SignalHandler:: Signal RegisteredSignal sending to DataPersistor.");
        if let Err(e) = self.quote_processor_addr.send(RegisteredSignal) {
            eprintln!("{}", e)
        }
        println!("SignalHandler:: Signal RegisteredSignal sending to QuoteProcessor.");
        if let Err(e) = self.quote_requester_addr.send(RegisteredSignal) {
            eprintln!("{}", e)
        }
        println!("SignalHandler:: Signal RegisteredSignal sending to QuoteRequester.");
        Ok(())
    }
}

#[message]
#[derive(Clone, Debug)]
struct RegisteredSignal;

#[async_trait::async_trait]
impl Handler<RegisteredSignal> for SignalHandler {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: RegisteredSignal) {
        println!("Signal handler::RegisteredSignal received RegisteredSignal message. exiting");
        // Implement behavior you want to execute when the signal is detected
        ctx.stop(None);
    }
}

#[xactor::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    println!("starting");
    let mut from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let time_interval = chrono::Duration::days(
        opts.interval
            .parse()
            .expect("Couldn;t parse 'interval' hours"),
    );
    let days = time_interval;
    let mut interval = stream::interval(Duration::from_secs(4));
    let data_persistor = DataPersistor::new(
        format!("{}.csv", Utc::now().to_rfc2822()),
        days.num_days() as usize,
    );
    let data_persistor_addr = data_persistor.start().await.unwrap();
    let data_persistor_addr_clone = data_persistor_addr.clone();

    let quote_processor_addr = QuoteProcessor {
        data_persistor_addr,
    }
    .start()
    .await
    .unwrap();
    let quote_processor_addr_clone = quote_processor_addr.clone();

    let quote_requester_addr = QuoteRequester {
        quote_processor_addr,
    }
    .start()
    .await
    .unwrap();

    let quote_requester_addr_clone = quote_requester_addr.clone();
    async_std::task::spawn(async move {
        let _signal_handler_addr = SignalHandler::new(
            data_persistor_addr_clone,
            quote_processor_addr_clone,
            quote_requester_addr_clone,
        )
        .start()
        .await;
    });

    let symbols: Vec<String> = opts.symbols.split(',').map(|s| s.to_owned()).collect();

    println!(
        "period start,symbol,price,change %,min,max,{}d avg",
        time_interval.num_days() as usize
    );

    let mut stopped = false;
    while interval.next().await.is_some() {
        if stopped {
            break;
        }
        let to: DateTime<Utc> = from + time_interval;

        for symbol in &symbols {
            if let Err(e) = quote_requester_addr.send(QuoteRequest {
                symbol: symbol.clone(),
                from,
                to,
            }) {
                eprint!("{}", e);
                stopped = true;
            }
        }
        from = to;
    }

    println!("Gracefully terminated");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]
    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[async_std::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal
                .calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0])
                .await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[async_std::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
