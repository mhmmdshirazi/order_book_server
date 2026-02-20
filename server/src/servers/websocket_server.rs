use crate::{
    listeners::trades::hl_listen,
    prelude::*,
    types::{
        Trade,
        subscription::{ClientMessage, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{env::home_dir, sync::Arc};
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
    let mut manager = SubscriptionManager::default();

    loop {
        select! {
            recv_result = trade_rx.recv() => {
                match recv_result {
                    Ok(trade) => {
                        for sub in manager.subscriptions() {
                            send_ws_data_from_trade(&mut socket, sub, &trade).await;
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

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut manager, value).await;
                            } else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription,
    };

    if !matches!(subscription, Subscription::Trades { coin } if !coin.is_empty()) {
        let msg = ServerResponse::Error(
            "Only non-empty trades subscriptions are supported by this websocket server".to_string(),
        );
        send_socket_message(socket, msg).await;
        return;
    }

    let sub = serde_json::to_string(subscription).unwrap_or_default();
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { subscription } => ("", manager.subscribe(subscription.clone())),
        ClientMessage::Unsubscribe { subscription } => ("un", manager.unsubscribe(subscription.clone())),
    };

    if success {
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
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

async fn send_ws_data_from_trade(socket: &mut WebSocket, subscription: &Subscription, trade: &Arc<Trade>) {
    if let Subscription::Trades { coin } = subscription {
        if coin == &trade.coin {
            let msg = ServerResponse::Trades(vec![trade.as_ref().clone()]);
            send_socket_message(socket, msg).await;
        }
    }
}
