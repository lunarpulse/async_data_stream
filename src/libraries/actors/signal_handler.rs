use async_std::channel;
use std::time::Duration;
use xactor::*;

use crate::libraries::actors::common::RegisteredSignal;

use super::{
    data_persistor::DataPersistor, quote_processor::QuoteProcessor, quote_requester::QuoteRequester,
};

pub struct SignalHandler {
    signal_rx: channel::Receiver<()>,
    data_persistor_addr: Addr<DataPersistor>,
    quote_processor_addr: Addr<QuoteProcessor>,
    quote_requester_addr: Addr<QuoteRequester>,
}

impl SignalHandler {
    pub fn new(
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

#[async_trait::async_trait]
impl Handler<RegisteredSignal> for SignalHandler {
    async fn handle(&mut self, ctx: &mut Context<Self>, _msg: RegisteredSignal) {
        println!("Signal handler::RegisteredSignal received RegisteredSignal message. exiting");
        // Implement behavior you want to execute when the signal is detected
        ctx.stop(None);
    }
}
