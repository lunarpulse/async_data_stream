use xactor::*;

use crate::libraries::{actors::common::FileFormat, sigal_process::*};

use super::{
    common::{Quotes, RegisteredSignal},
    data_persistor::DataPersistor,
};

pub struct QuoteProcessor {
    pub data_persistor_addr: Addr<DataPersistor>,
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
