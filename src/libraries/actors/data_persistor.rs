use std::fs::File;
use std::io::BufWriter;

use std::io::Write;
use xactor::*;

use super::common::FileFormat;
use super::common::RegisteredSignal;
pub struct DataPersistor {
    pub filename: String,
    pub writer: Option<BufWriter<File>>,
    count: i32,
    days: usize,
}
impl DataPersistor {
    pub fn new(filename: String, days: usize) -> Self {
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
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        let mut file = File::create(&self.filename)
            .unwrap_or_else(|_| panic!("Could not open file '{}'", self.filename));
        let _ = writeln!(
            &mut file,
            "period start,symbol,price,change %,min,max,{}d avg",
            self.days
        );
        self.writer = Some(BufWriter::new(file));
        ctx.subscribe::<RegisteredSignal>().await
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        if let Some(writer) = &mut self.writer {
            writer.flush().expect("unable to flush")
        };
        ctx.stop(None);
    }
}
