use crate::{
    listeners::trades::hl_listen,
    prelude::*,
    types::{
        Trade,
        subscription::{ClientMessage, ServerResponse, Subscription},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use serde::Serialize;
use std::{collections::HashSet, env::home_dir, sync::Arc};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::broadcast::{Sender, channel},
};
use yawc::{FrameView, OpCode, WebSocket};

pub async fn run_websocket_server(address: &str, _ignore_spot: bool, compression_level: u32) -> Result<()> {
    let (trade_tx, _) = channel::<Arc<Trade>>(10_000);

    let home_dir = home_dir().ok_or("Could not find home directory")?;
    {
        let trade_tx = trade_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(trade_tx, home_dir).await {
                error!("Trade relay listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let trade_tx = trade_tx.clone();
            async move |ws_upgrade| ws_handler(ws_upgrade, trade_tx.clone(), websocket_opts)
        }),
    );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    trade_tx: Sender<Arc<Trade>>,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, trade_tx).await;
    });

    resp
}

async fn handle_socket(mut socket: WebSocket, trade_tx: Sender<Arc<Trade>>) {
    let mut trade_rx = trade_tx.subscribe();
    let mut subscribed_coins = HashSet::<String>::new();

    loop {
        select! {
            recv_result = trade_rx.recv() => {
                match recv_result {
                    Ok(trade) => {
                        if subscribed_coins.contains(trade.coin.as_str()) {
                            send_trade_message(&mut socket, &trade).await;
                        }
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }
            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    error!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    return;
                                }
                            };

                            debug!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut subscribed_coins, value).await;
                            } else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            debug!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    debug!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    subscribed_coins: &mut HashSet<String>,
    client_message: ClientMessage,
) {
    match client_message {
        ClientMessage::Subscribe { subscription } => {
            let sub = serde_json::to_string(&subscription).unwrap_or_default();
            let coin = match subscription {
                Subscription::Trades { coin } if !coin.is_empty() => coin,
                _ => {
                    let msg = ServerResponse::Error(
                        "Only non-empty trades subscriptions are supported by this websocket server".to_string(),
                    );
                    send_socket_message(socket, msg).await;
                    return;
                }
            };
            if subscribed_coins.insert(coin.clone()) {
                let msg = ServerResponse::SubscriptionResponse(ClientMessage::Subscribe {
                    subscription: Subscription::Trades { coin },
                });
                send_socket_message(socket, msg).await;
            } else {
                let msg = ServerResponse::Error(format!("Already subscribed: {sub}"));
                send_socket_message(socket, msg).await;
            }
        }
        ClientMessage::Unsubscribe { subscription } => {
            let sub = serde_json::to_string(&subscription).unwrap_or_default();
            let coin = match subscription {
                Subscription::Trades { coin } if !coin.is_empty() => coin,
                _ => {
                    let msg = ServerResponse::Error(
                        "Only non-empty trades subscriptions are supported by this websocket server".to_string(),
                    );
                    send_socket_message(socket, msg).await;
                    return;
                }
            };
            if subscribed_coins.remove(&coin) {
                let msg = ServerResponse::SubscriptionResponse(ClientMessage::Unsubscribe {
                    subscription: Subscription::Trades { coin },
                });
                send_socket_message(socket, msg).await;
            } else {
                let msg = ServerResponse::Error(format!("Already unsubscribed: {sub}"));
                send_socket_message(socket, msg).await;
            }
        }
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

#[derive(Serialize)]
struct SingleTradeMessage<'a> {
    channel: &'static str,
    data: [&'a Trade; 1],
}

async fn send_trade_message(socket: &mut WebSocket, trade: &Trade) {
    let msg = SingleTradeMessage { channel: "trades", data: [trade] };
    match serde_json::to_string(&msg) {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}
