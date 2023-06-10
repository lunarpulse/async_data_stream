use chrono::prelude::*;
use std::time::Duration;
use xactor::*;

use super::common::{InitiateRequest, QuoteRequest, RegisteredSignal};
use super::quote_requester::QuoteRequester;

pub struct QuoteMaker {
    pub quote_requester_addr: Addr<QuoteRequester>,
    pub ticker: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub interval: chrono::Duration,
}

impl QuoteMaker {
    pub async fn new(
        ticker: String,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        interval: chrono::Duration,
    ) -> Self {
        QuoteMaker {
            quote_requester_addr: QuoteRequester {}.start().await.unwrap(),
            ticker,
            from,
            to,
            interval,
        }
    }
}

#[async_trait::async_trait]
impl Handler<InitiateRequest> for QuoteMaker {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: InitiateRequest) {
        let date_to = self.from + self.interval;
        if date_to > self.to {
            ctx.send_later(RegisteredSignal, Duration::from_micros(1));
        }
        let request = QuoteRequest {
            symbol: self.ticker.clone(),
            from: self.from,
            to: date_to,
        };
        self.from = date_to;
        if let Err(e) = self.quote_requester_addr.send(request) {
            eprint!("{}", e);
        }
    }
}

#[async_trait::async_trait]
impl Handler<RegisteredSignal> for QuoteMaker {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: RegisteredSignal) {
        println!("QuoteMaker:: RegisteredSignal terminating the actor");
        ctx.stop(None)
    }
}

#[async_trait::async_trait]
impl Actor for QuoteMaker {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        println!("QuoteMaker {} Started", self.ticker);
        ctx.send_interval(InitiateRequest, Duration::from_secs(10));
        ctx.subscribe::<RegisteredSignal>().await
    }

    async fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("QuoteMaker:: {} stopped()", self.ticker);
    }
}
