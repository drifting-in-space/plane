use super::Controller;
use crate::{
    protocol::{MessageFromDns, MessageToDns},
    typed_socket::{server::TypedWebsocketServer, FullDuplexChannel},
    types::NodeStatus,
};
use axum::{
    extract::{ws::WebSocket, ConnectInfo, State, WebSocketUpgrade},
    response::IntoResponse,
};
use std::net::{IpAddr, SocketAddr};

pub async fn dns_socket_inner(
    ws: WebSocket,
    controller: Controller,
    ip: IpAddr,
) -> anyhow::Result<()> {
    let mut socket =
        TypedWebsocketServer::<MessageToDns>::new(ws, controller.id.to_string()).await?;

    let handshake = socket.remote_handshake().clone();
    let node_guard = controller.register_node(handshake, None, ip).await?;

    // TODO: this is a fake heartbeat until we decide how the proxy heartbeat should work.
    controller
        .db
        .node()
        .heartbeat(node_guard.id, NodeStatus::Available)
        .await?;

    loop {
        let message_from_dns_result = socket.recv().await;
        tracing::info!(?message_from_dns_result, "Handling message from DNS...");
        match message_from_dns_result {
            Some(MessageFromDns::TxtRecordRequest { cluster }) => {
                let txt_value = match controller.db.acme().txt_record_for_cluster(&cluster).await {
                    Ok(txt_value) => txt_value,
                    Err(err) => {
                        tracing::error!(?err, "Error getting txt record");
                        continue;
                    }
                };

                if let Err(err) = socket
                    .send(&MessageToDns::TxtRecordResponse { cluster, txt_value })
                    .await
                {
                    tracing::error!(?err, "Error sending txt record response to drone.");
                }
            }
            None => {
                tracing::info!("DNS socket closed");
                break;
            }
        }
    }

    Ok(())
}

pub async fn dns_socket(ws: WebSocket, controller: Controller, ip: IpAddr) {
    if let Err(err) = dns_socket_inner(ws, controller, ip).await {
        tracing::error!(?err, "Error handling proxy socket");
    }
}

pub async fn handle_dns_socket(
    State(controller): State<Controller>,
    connect_info: ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let ip = connect_info.ip();
    ws.on_upgrade(move |socket| dns_socket(socket, controller, ip))
}