use chrono::prelude::*;
use clap::Parser;
use std::fs;
use std::io::Read;
use std::iter::Iterator;
use std::process::exit;
use trade_utils::libraries::actors::common::QuoteDetails;
use trade_utils::libraries::actors::data_persistor::DataPersistor;
use trade_utils::libraries::actors::quote_maker::QuoteMaker;
use trade_utils::libraries::actors::quote_processor::QuoteProcessor;
use trade_utils::libraries::actors::signal_handler::SignalHandler;
use xactor::*;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long)]
    start: String,
    #[clap(short, long)]
    end: String,
    #[clap(short, long)]
    interval: String,
    #[clap(short, long)]
    tickers: String,
}

fn read_file_to_str(filename: &str) -> std::io::Result<String> {
    // Open the file
    let mut file = fs::File::open(filename)?;

    // Allocate a buffer to store the contents
    let mut contents = String::new();

    // Read the file contents into the buffer
    file.read_to_string(&mut contents)?;

    // Return the contents
    Ok(contents)
}

#[xactor::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    println!("starting");
    let date_from: DateTime<Utc> = opts.start.parse().expect("Couldn't parse 'from' date");
    let date_to: DateTime<Utc> = opts.end.parse().expect("Couldn't parse 'to' date");

    let time_interval = chrono::Duration::days(
        opts.interval
            .parse()
            .expect("Couldn;t parse 'interval' hours"),
    );
    let days = time_interval;
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

    async_std::task::spawn(async move {
        let _signal_handler_addr =
            SignalHandler::new(data_persistor_addr_clone, quote_processor_addr_clone)
                .start()
                .await;
    });

    println!(
        "period start,symbol,price,change %,min,max,{}d avg",
        time_interval.num_days() as usize
    );

    let tickers: String = opts.tickers.parse().unwrap();
    let symbols = match read_file_to_str(&tickers) {
        Ok(contents) => {
            println!("File contents: {}", contents);
            let symbols: Vec<String> = contents.split(',').map(|s| s.to_owned()).collect();
            symbols
        }
        Err(e) => {
            println!("Error reading file: {}", e);
            exit(2);
        }
    };

    let qoute_details: Vec<QuoteDetails> = symbols
        .into_iter()
        .map(|symbol| QuoteDetails {
            ticker: symbol,
            from: date_from,
            to: date_to,
            interval: time_interval,
        })
        .collect();

    let qoute_makers = qoute_details.into_iter().map(move |details| {
        QuoteMaker::new(details.ticker, details.from, details.to, details.interval)
    });

    let quote_makers_started = qoute_makers
        .into_iter()
        .map(|actor| async { actor.await.start().await.unwrap().wait_for_stop().await });

    let _quote_makers_future = futures::future::join_all(quote_makers_started).await;

    println!("Application Gracefully stopped");

    Ok(())
}
