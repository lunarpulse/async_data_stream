use chrono::prelude::*;
use xactor::message;
use yahoo_finance_api as yahoo;

#[message]
#[derive(Clone, Debug)]
pub struct FileFormat {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub pct_change: f64,
    pub period_min: f64,
    pub period_max: f64,
    pub last_sma: f64,
}

#[message]
#[derive(Debug, Clone)]
pub struct Quotes {
    pub days: usize,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub quotes: Vec<yahoo::Quote>,
}

#[message]
#[derive(Debug, Clone)]
pub struct QuoteRequest {
    pub symbol: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

#[message]
#[derive(Clone, Debug)]
pub struct RegisteredSignal;

#[message]
#[derive(Debug, Clone)]
pub struct QuoteDetails {
    pub ticker: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub interval: chrono::Duration,
}

#[message]
#[derive(Debug, Clone)]
pub struct InitiateRequest;
