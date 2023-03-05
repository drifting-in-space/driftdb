use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Serialize};
use wasm_bindgen::prelude::Closure;
use wasm_bindgen::JsCast;
use web_sys::{MessageEvent, WebSocket};

#[derive(Debug)]
pub struct WebSocketConnection<Inbound: DeserializeOwned, Outbound: Serialize> {
    socket: WebSocket,
    _message_handler: Closure<dyn FnMut(MessageEvent)>,
    _ph_i: std::marker::PhantomData<Inbound>,
    _ph_o: std::marker::PhantomData<Outbound>,
}

enum Message {
    Text(String),
    Bytes(Vec<u8>),
}

impl<Inbound: DeserializeOwned, Outbound: Serialize> WebSocketConnection<Inbound, Outbound> {
    pub fn new<F>(url: &str, callback: F) -> Result<Self>
    where
        F: Fn(Inbound) + 'static,
    {
        let ws =
            WebSocket::new(url).map_err(|err| anyhow!("Error creating websocket. {:?}", err))?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let message_handler = Closure::<dyn FnMut(_)>::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                // let array = js_sys::Uint8Array::new(&abuf);
                // let array = array.to_vec();

                log::info!("message event, received bytes: {:?}", abuf);
            } else if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
                let txt = txt.as_string().unwrap();

                callback(serde_json::from_str(&txt).unwrap());
            } else {
                log::warn!("message event, received Unknown: {:?}", e.data());
            }
        }));

        ws.set_onmessage(Some(message_handler.as_ref().unchecked_ref()));

        Ok(WebSocketConnection {
            socket: ws,
            _message_handler: message_handler,
            _ph_i: std::marker::PhantomData,
            _ph_o: std::marker::PhantomData,
        })
    }

    pub fn send(&self, message: &Outbound) {
        let txt = serde_json::to_string(message).unwrap();
        self.socket.send_with_str(&txt).unwrap();
    }
}
