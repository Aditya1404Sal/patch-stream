mod bindings {
    wit_bindgen::generate!({
        world: "meta-json",
        path: "../wit",
        generate_all,
        async: [
            "export:wasmcloud:patch-stream/sink@0.1.0#send-stream",
            "export:betty-blocks:websockets/handler@0.1.0#handle",
            "import:betty-blocks:stream-broker/broker@0.1.0#wait-message",
            "import:betty-blocks:stream-broker/broker@0.1.0#publish-message",
        ],
    });
}

use bindings::betty_blocks::stream_broker::broker;
use bindings::betty_blocks::websockets::types::{Frame, UpgradeRequest};
use bindings::exports::betty_blocks::websockets::handler::Guest as WsGuest;
use bindings::exports::wasmcloud::patch_stream::sink::Guest as SinkGuest;
use bindings::wasmcloud::patch_stream::page_generation;
use wit_bindgen::StreamReader;

struct Component;

// ---- Existing sink path (HTTP-NDJSON demo, kept for parity) ----

impl SinkGuest for Component {
    async fn send_stream(
        mut s: StreamReader<u8>,
        control: page_generation::PageStream,
    ) -> Result<(), ()> {
        eprintln!("meta-json: stream received, draining…");
        let start_cancel_generation = broker::cancel_generation();
        let mut cancelled = false;
        let mut line: Vec<u8> = Vec::with_capacity(256);
        let mut bytes: u64 = 0;
        let mut lines: u64 = 0;
        while let Some(byte) = s.next().await {
            if !cancelled && broker::cancel_generation() != start_cancel_generation {
                let before = control.status();
                control.cancel_streaming();
                let after = control.status();
                eprintln!(
                    "meta-json: cancellation requested; page-agent status {before} -> {after}"
                );
                cancelled = true;
            }

            bytes += 1;
            if byte == b'\n' {
                lines += 1;
                let text = String::from_utf8_lossy(&line).into_owned();
                eprintln!("meta-json: [{lines:>3}] {text}");
                broker::publish_message(text).await.map_err(|err| {
                    eprintln!("meta-json: broker publish failed: {err}");
                })?;
                line.clear();
            } else {
                line.push(byte);
            }
        }
        if !cancelled && broker::cancel_generation() != start_cancel_generation {
            let before = control.status();
            control.cancel_streaming();
            let after = control.status();
            eprintln!(
                "meta-json: cancellation requested after drain; page-agent status {before} -> {after}"
            );
            cancelled = true;
        }
        if !line.is_empty() {
            lines += 1;
            let text = String::from_utf8_lossy(&line).into_owned();
            eprintln!("meta-json: [{lines:>3}] {text} (no trailing newline)",);
            broker::publish_message(text).await.map_err(|err| {
                eprintln!("meta-json: broker publish failed: {err}");
            })?;
        }
        eprintln!(
            "meta-json: stream closed after {bytes} bytes / {lines} patches cancelled={cancelled}"
        );
        Ok(())
    }
}

// ---- WebSocket path ----

impl WsGuest for Component {
    async fn handle(
        req: UpgradeRequest,
        mut incoming: StreamReader<Frame>,
    ) -> Result<StreamReader<Frame>, String> {
        eprintln!(
            "meta-json[ws]: connection opened path={:?} subprotocols={:?}",
            req.path, req.subprotocols
        );

        let (mut outgoing_tx, outgoing_rx) = bindings::wit_stream::new::<Frame>();

        // Drain incoming frames in the background so the connection
        // stays observable bidirectionally. We only log; nothing in
        // this demo reacts to client-sent frames.
        wit_bindgen::spawn(async move {
            while let Some(frame) = incoming.next().await {
                match frame {
                    Frame::Text(s) => {
                        eprintln!("meta-json[ws]: recv text: {s}");
                        if s.trim().eq_ignore_ascii_case("cancel") {
                            let generation = broker::request_cancel();
                            eprintln!(
                                "meta-json[ws]: requested cancellation generation={generation}"
                            );
                        }
                    }
                    Frame::Binary(b) => {
                        eprintln!("meta-json[ws]: recv binary {} bytes", b.len())
                    }
                    Frame::Close(info) => {
                        eprintln!(
                            "meta-json[ws]: recv close code={} reason={:?}",
                            info.code, info.reason
                        );
                        break;
                    }
                }
            }
            eprintln!("meta-json[ws]: incoming stream closed");
        });

        // Keep the websocket open and forward messages published by
        // the sink entrypoint. This bridges separate incoming
        // requests: websocket connects first, then commander later
        // invokes sink::send-stream over HTTP.
        wit_bindgen::spawn(async move {
            let client_id = broker::register();
            let mut lines: u64 = 0;
            while let Some(text) = broker::wait_message(client_id).await {
                lines += 1;
                outgoing_tx.write_all(vec![Frame::Text(text)]).await;
            }
            broker::unregister(client_id);
            eprintln!("meta-json[ws]: sent {lines} text frames; closing");
            // outgoing_tx drops here → host closes WS with 1000.
        });

        Ok(outgoing_rx)
    }
}

bindings::export!(Component with_types_in bindings);
