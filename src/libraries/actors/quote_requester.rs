use xactor::*;
use yahoo_finance_api as yahoo;

use super::{
    common::{QuoteRequest, Quotes, RegisteredSignal},
    quote_processor::QuoteProcessor,
};

pub struct QuoteRequester {
    pub quote_processor_addr: Addr<QuoteProcessor>,
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
