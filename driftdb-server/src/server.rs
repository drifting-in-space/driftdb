use crate::Opts;
use anyhow::Result;
use axum::{
    body::BoxBody,
    extract::{ws::WebSocket, Host, Path, Query, State, WebSocketUpgrade},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use driftdb::{Database, MessageFromDatabase, MessageToDatabase};
use hyper::{Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::Level;
use uuid::Uuid;
use hyper::http::header;

struct TypedWebSocket<Inbound: DeserializeOwned, Outbound: Serialize> {
    socket: WebSocket,
    _ph_inbound: std::marker::PhantomData<Inbound>,
    _ph_outbound: std::marker::PhantomData<Outbound>,
}

impl<Inbound: DeserializeOwned, Outbound: Serialize> TypedWebSocket<Inbound, Outbound> {
    pub fn new(socket: WebSocket) -> Self {
        Self {
            socket,
            _ph_inbound: std::marker::PhantomData,
            _ph_outbound: std::marker::PhantomData,
        }
    }

    pub async fn recv(&mut self) -> Result<Option<Inbound>> {
        let msg = self.socket.recv().await.transpose()?;
        loop {
            match &msg {
                Some(msg) => match msg {
                    axum::extract::ws::Message::Close(_) => {
                        return Ok(None);
                    }
                    axum::extract::ws::Message::Ping(_) => {
                        self.socket
                            .send(axum::extract::ws::Message::Pong(vec![]))
                            .await?;
                    }
                    axum::extract::ws::Message::Pong(_) => {}
                    axum::extract::ws::Message::Binary(_) => {
                        return Err(anyhow::anyhow!("Binary messages are not supported."));
                    }
                    axum::extract::ws::Message::Text(msg) => {
                        let msg = serde_json::from_str(&msg)?;
                        return Ok(Some(msg));
                    }
                },
                None => return Ok(None),
            }
        }
    }

    pub async fn send(&mut self, msg: Outbound) -> Result<()> {
        let msg = serde_json::to_string(&msg)?;
        self.socket
            .send(axum::extract::ws::Message::Text(msg))
            .await?;
        Ok(())
    }
}

async fn handle_socket(socket: WebSocket, database: Arc<Database>, debug: bool, replica: bool) {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
    let mut socket: TypedWebSocket<MessageToDatabase, MessageFromDatabase> =
        TypedWebSocket::new(socket);

    let callback = move |message: &MessageFromDatabase| {
        let result = sender.try_send(message.clone());

        if let Err(err) = result {
            tracing::error!(
                ?err,
                "Failed to send message to user, probably already closed."
            );
        }
    };

    let conn = if debug {
        database.connect_debug(callback)
    } else if replica {
        database.connect_replica(callback)
    } else {
        database.connect(callback)
    };

    loop {
        tokio::select! {
            msg = receiver.recv() => {
                // We've received a message from the database; forward it to user.

                let msg = msg.expect("Receiver should never be dropped before socket is closed.");

                socket.send(msg).await.expect("Failed to send message to user.");
            }
            msg = socket.recv() => {
                // We've received a message from the client; forward it to the database.

                match msg {
                    Ok(Some(msg)) => {
                        if let Err(e) = conn.send_message(&msg) {
                            tracing::error!(?e, "Failed to send message to database.");

                            let _ = socket.send(MessageFromDatabase::Error {
                                message: format!("Failed to send message to database: {}", e),
                            }).await;
                        }
                    },
                    Ok(None) => {
                        // Client has closed the connection.
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(?err, "Failed to receive message from user.");

                        let _ = socket.send(MessageFromDatabase::Error {
                            message: format!("Failed to receive message from user: {}", err),
                        }).await;

                        break;
                    }
                };
            }
        }
    }
}

#[derive(Deserialize)]
struct ConnectionQuery {
    #[serde(default)]
    debug: bool,

    #[serde(default)]
    replica: bool,
}

type RoomMap = DashMap<String, Arc<Database>>;

async fn post_message(
    Path(room_id): Path<String>,
    State(room_map): State<Arc<RoomMap>>,
    Json(msg): Json<MessageToDatabase>,
) -> std::result::Result<Json<Option<MessageFromDatabase>>, StatusCode> {
    let database = room_map
        .get(&room_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    let result = database.send_message(&msg);

    Ok(Json(result))
}

async fn connection(
    Path(room_id): Path<String>,
    ws: WebSocketUpgrade,
    State(room_map): State<Arc<RoomMap>>,
    Query(query): Query<ConnectionQuery>,
) -> Response<BoxBody> {
    let database = room_map
        .get(&room_id)
        .expect("Room should have been created before connection.")
        .clone();

    ws.on_upgrade(move |socket| handle_socket(socket, database, query.debug, query.replica))
}

async fn new_room(Host(hostname): Host, State(room_map): State<Arc<RoomMap>>) -> Json<RoomResult> {
    let room = Uuid::new_v4().to_string();
    let database = Arc::new(Database::new());
    room_map.insert(room.clone(), database);

    let result = RoomResult::new(room, &hostname);

    Json(result)
}

async fn room(
    Path(room_id): Path<String>,
    State(room_map): State<Arc<RoomMap>>,
    Host(hostname): Host,
) -> std::result::Result<Json<RoomResult>, StatusCode> {
    let _ = room_map.get(&room_id).ok_or(StatusCode::NOT_FOUND)?;

    let result = RoomResult::new(room_id, &hostname);

    Ok(Json(result))
}

#[derive(Serialize)]
struct RoomResult {
    room: String,
    socket_url: String,
    http_url: String,
}

impl RoomResult {
    fn new(room: String, hostname: &str) -> Self {
        let socket_url = format!("ws://{}/room/{}/connect", hostname, room);
        let http_url = format!("http://{}/room/{}/send", hostname, room);

        Self {
            room,
            socket_url,
            http_url,
        }
    }
}

pub fn api_routes() -> Result<Router> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(vec![
            header::AUTHORIZATION,
            header::ACCEPT,
            header::CONTENT_TYPE,
        ])
        .allow_origin(AllowOrigin::any());

    let room_map = RoomMap::new();

    Ok(Router::new()
        .route("/new", post(new_room))
        .route("/room/:room_id/connect", get(connection))
        .route("/room/:room_id/send", post(post_message))
        .route("/room/:room_id", get(room))
        .layer(cors)
        .with_state(Arc::new(room_map)))
}

pub async fn run_server(opts: &Opts) -> anyhow::Result<()> {
    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_request(DefaultOnRequest::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO));

    let app = api_routes()?.layer(trace_layer);
    let addr = SocketAddr::new(opts.host, opts.port);

    tracing::info!(?addr, "Server is listening.");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Err(anyhow::anyhow!("Server exited."))
}
