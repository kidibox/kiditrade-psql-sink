#[macro_use]
extern crate log;
#[macro_use]
extern crate diesel;

use std::str;

use clap::{App, Arg};
use env_logger::Env;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use bigdecimal::BigDecimal;
use diesel::pg::PgConnection;
use diesel::prelude::*;

use chrono::{DateTime, Utc};
use serde::Deserialize;

pub mod schema;
use schema::bars;

#[derive(Deserialize, Insertable, Debug)]
#[table_name = "bars"]
struct Bar {
    symbol: String,
    time: DateTime<Utc>,
    size: i32,
    open: BigDecimal,
    high: BigDecimal,
    low: BigDecimal,
    close: BigDecimal,
    wap: BigDecimal,
    volume: i64,
    trades: i64,
}

fn pg_connect(database_url: &str) -> PgConnection {
    PgConnection::establish(database_url).expect(&format!("Error connecting to {}", database_url))
}

fn main() {
    let env = Env::default().default_filter_or("info");
    env_logger::init_from_env(env);

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .about("Write candles to postgresql")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group-id")
                .takes_value(true)
                .default_value("kiditrade-psql-sink"),
        )
        .arg(
            Arg::with_name("topic")
                .short("t")
                .long("topic")
                .help("Kafka topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("database-url")
                .short("d")
                .long("database-url")
                .takes_value(true)
                .env("DATABASE_URL")
                .required(true),
        )
        .get_matches();

    let topic = matches.value_of("topic").unwrap().to_owned();
    let group_id = matches.value_of("group-id").unwrap().to_owned();
    let brokers = matches.value_of("brokers").unwrap().to_owned();
    let database_url = matches.value_of("database-url").unwrap().to_owned();

    let mut consumer = Consumer::from_hosts(vec![brokers])
        .with_topic(topic)
        .with_group(group_id)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    let connection = pg_connect(&database_url);
    use schema::bars::dsl::*;

    loop {
        for ms in consumer.poll().unwrap().iter() {
            let values: Result<Vec<Bar>, _> = ms
                .messages()
                .iter()
                .map(|m| m.value)
                .map(serde_json::from_slice::<Bar>)
                .collect();

            match values {
                Ok(v) => {
                    let r = diesel::insert_into(bars)
                        .values(v)
                        .on_conflict_do_nothing()
                        .execute(&connection);
                    info!("Saved {} bars", r.unwrap());
                    consumer.consume_messageset(ms).unwrap();
                }
                Err(e) => warn!("Error parsing: {}", e),
            }
        }

        consumer.commit_consumed().unwrap();
    }
}
