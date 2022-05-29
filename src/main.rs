use futures_util::{SinkExt, StreamExt};
use libc::{c_int, sighandler_t, signal, SIGINT, SIGTERM};
use log::info;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::codec::Decoder;

use std::{
    env, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use crate::{
    cmd::CommandParser,
    db::Db,
    proto::{RedisError, RedisProtocol, Value},
};

mod cmd;
mod db;
mod proto;

async fn run() -> Result<(), io::Error> {
    info!("Initializing database");

    let db = Db::new();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 6379);

    let listener = TcpListener::bind(addr).await?;

    info!("Listening on {addr}");

    while let Ok((stream, client_addr)) = listener.accept().await {
        info!("Client connected from {client_addr}");

        tokio::spawn(handle(stream, db.clone()));
    }

    Ok(())
}

async fn handle(stream: TcpStream, db: Db) -> Result<(), io::Error> {
    let stream = RedisProtocol.framed(stream);
    let (mut sink, mut stream) = stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            if sink.send(item).await.is_err() {
                break;
            };
        }
    });

    while let Some(Ok(item)) = stream.next().await {
        let db = db.clone();
        let tx = tx.clone();

        tokio::spawn(async move {
            let reply = if let Value::Array(buffer) = item {
                let parser = CommandParser::new(buffer);

                if let Ok(command) = parser.parse() {
                    command.apply(&db).await
                } else {
                    Value::Error(RedisError {
                        message: String::from("Failed to parse command"),
                    })
                }
            } else {
                Value::Error(RedisError {
                    message: String::from("Failed to parse command"),
                })
            };

            let _ = tx.send(reply);
        });
    }

    Ok(())
}

pub extern "C" fn handler(_: c_int) {
    std::process::exit(0);
}

unsafe fn set_os_handlers() {
    signal(SIGINT, handler as extern "C" fn(_) as sighandler_t);
    signal(SIGTERM, handler as extern "C" fn(_) as sighandler_t);
}

fn main() -> Result<(), io::Error> {
    unsafe { set_os_handlers() };

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }

    env_logger::init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run())
}
