use async_std::stream;
use async_std::stream::StreamExt;
use chrono::prelude::*;
use clap::Parser;
use std::fs;
use std::io::Read;
use std::iter::Iterator;
use std::process::exit;
use std::time::Duration;
use trade_utils::libraries::actors::common::QuoteRequest;
use trade_utils::libraries::actors::data_persistor::DataPersistor;
use trade_utils::libraries::actors::quote_processor::QuoteProcessor;
use trade_utils::libraries::actors::quote_requester::QuoteRequester;
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
    symbols: Option<String>,
    #[clap(short, long)]
    from: String,
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
    let mut from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let time_interval = chrono::Duration::days(
        opts.interval
            .parse()
            .expect("Couldn;t parse 'interval' hours"),
    );
    let days = time_interval;
    let mut interval = stream::interval(Duration::from_secs(30));
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

    //let symbols: Vec<String> = opts.symbols.split(',').map(|s| s.to_owned()).collect();

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
            if let Err(_e) = quote_requester_addr.send(QuoteRequest {
                symbol: symbol.clone(),
                from,
                to,
            }) {
                // eprint!("{}", e);
                stopped = true;
            }
        }
        from = to;
    }

    println!("Gracefully terminated");
    Ok(())
}
