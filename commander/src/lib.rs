mod bindings {
    wit_bindgen::generate!({
        world: "commander",
        path: "../wit",
        generate_all,
        async: [
            "import:wasmcloud:patch-stream/page-generation@0.1.0#generate-page",
            "import:wasmcloud:patch-stream/sink@0.1.0#send-stream",
            "import:wasmcloud:patch-stream/broker@0.1.0#wait-cancel-after",
            "export:wasi:http/handler@0.3.0-rc-2026-03-15#handle",
        ],
    });
}

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::patch_stream::{broker, page_generation, sink};
use futures::future::{Either, select};

struct Component;

/// Single-file flow simulator served on bare `GET /` (no `?prompt=`).
/// Pattern borrowed from examples/blobby — `include_str!` bakes the
/// HTML into the wasm so the demo is self-contained: one `wash dev`,
/// one open browser tab.
static UI_HTML: &str = include_str!("../../ui.html");

impl Handler for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        // Two behaviors on the same URL:
        //   GET /?prompt=...  → trigger page-agent → broker fan-out
        //   GET /             → serve the demo UI
        // (WS upgrades on GET / are routed to meta-json by the host
        // before we ever see them, so commander only handles plain HTTP.)
        let path = request.get_path_with_query();
        if is_cancel_path(path.as_deref()) {
            serve_cancel().await
        } else {
            match prompt_from_path(path.as_deref()) {
                Some(prompt) => serve_trigger(prompt).await,
                None => serve_ui().await,
            }
        }
    }
}

async fn serve_trigger(prompt: String) -> Result<Response, ErrorCode> {
    let headers = Fields::new();
    let _ = headers.append(
        &"content-type".to_string(),
        &b"text/plain; charset=utf-8".to_vec(),
    );

    // Kick off PageAgent and hand the resulting byte stream to meta-json in a
    // background task. Commander keeps the PageAgent control resource so it can
    // cancel the producer if a separate HTTP or websocket request increments
    // the broker cancellation generation.
    let (page_rx, control) = page_generation::generate_page(prompt).await;
    let start_cancel_generation = broker::cancel_generation();
    let (mut body_tx, body_rx) = bindings::wit_stream::new::<u8>();
    let (_trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));

    wit_bindgen::spawn(async move {
        body_tx.write_all(b"accepted\n".to_vec()).await;

        let send = sink::send_stream(page_rx);
        let cancel = broker::wait_cancel_after(start_cancel_generation);
        futures::pin_mut!(send);
        futures::pin_mut!(cancel);

        let message = match select(send, cancel).await {
            Either::Left((Ok(()), _cancel)) => "stream finished\n",
            Either::Left((Err(()), _cancel)) => "stream forwarding failed\n",
            Either::Right((_generation, send)) => {
                let before = control.status();
                control.cancel_streaming();
                let after = control.status();
                match send.await {
                    Ok(()) => {
                        let message =
                            format!("stream cancelled: page-agent status {before} -> {after}\n");
                        body_tx.write_all(message.into_bytes()).await;
                        return;
                    }
                    Err(()) => {
                        let message = format!(
                            "stream cancellation failed: page-agent status {before} -> {after}\n"
                        );
                        body_tx.write_all(message.into_bytes()).await;
                        return;
                    }
                }
            }
        };
        body_tx.write_all(message.as_bytes().to_vec()).await;
    });

    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response
        .set_status_code(202)
        .map_err(|()| ErrorCode::InternalError(Some("set_status failed".into())))?;
    Ok(response)
}

async fn serve_cancel() -> Result<Response, ErrorCode> {
    let generation = broker::request_cancel();
    text_response(202, format!("cancel requested generation={generation}\n")).await
}

async fn serve_ui() -> Result<Response, ErrorCode> {
    let headers = Fields::new();
    let _ = headers.append(
        &"content-type".to_string(),
        &b"text/html; charset=utf-8".to_vec(),
    );

    let (mut body_tx, body_rx) = bindings::wit_stream::new::<u8>();
    let (_trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));

    // Spawn the body-writer task. UI_HTML is a &'static str baked in
    // at compile time so the copy here is cheap (no I/O, no allocs
    // beyond the one Vec<u8>).
    wit_bindgen::spawn(async move {
        body_tx.write_all(UI_HTML.as_bytes().to_vec()).await;
    });

    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response
        .set_status_code(200)
        .map_err(|()| ErrorCode::InternalError(Some("set_status failed".into())))?;
    Ok(response)
}

async fn text_response(status: u16, body: String) -> Result<Response, ErrorCode> {
    let headers = Fields::new();
    let _ = headers.append(
        &"content-type".to_string(),
        &b"text/plain; charset=utf-8".to_vec(),
    );

    let (mut body_tx, body_rx) = bindings::wit_stream::new::<u8>();
    let (_trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    wit_bindgen::spawn(async move {
        body_tx.write_all(body.into_bytes()).await;
    });

    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response
        .set_status_code(status)
        .map_err(|()| ErrorCode::InternalError(Some("set_status failed".into())))?;
    Ok(response)
}

fn is_cancel_path(path_with_query: Option<&str>) -> bool {
    path_with_query
        .and_then(|path| path.split('?').next())
        .is_some_and(|path| path == "/cancel")
}

/// Returns `Some(prompt)` if the request has a non-empty `?prompt=...`
/// query parameter, otherwise `None` (caller treats `None` as "serve
/// the UI"). A previous version of this fell back to a hardcoded
/// DEFAULT_PROMPT for any bare `GET /`; that made it impossible to
/// distinguish a UI request from a trigger.
fn prompt_from_path(path_with_query: Option<&str>) -> Option<String> {
    let path_with_query = path_with_query?;
    let (_, query) = path_with_query.split_once('?')?;

    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(key, value)| {
            if key == "prompt" {
                Some(percent_decode(value))
            } else {
                None
            }
        })
        .filter(|prompt| !prompt.trim().is_empty())
}

fn percent_decode(input: &str) -> String {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = from_hex(bytes[i + 1]);
                let lo = from_hex(bytes[i + 2]);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi << 4) | lo);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

bindings::export!(Component with_types_in bindings);
