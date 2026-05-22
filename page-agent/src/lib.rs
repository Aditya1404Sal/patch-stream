mod bindings {
    wit_bindgen::generate!({
        world: "page-agent",
        path: "../wit",
        generate_all,
        async: [
            "export:wasmcloud:patch-stream/page-generation@0.1.0#generate-page",
            "import:wasi:clocks/monotonic-clock@0.3.0-rc-2026-03-15#wait-for",
            "import:wasi:http/client@0.3.0-rc-2026-03-15#send",
        ],
    });
}

const OPENAI_API_KEY: &str = ""; //put the key here since wash dev does not support  env variable passing yet

use bindings::exports::wasmcloud::patch_stream::page_generation::{
    Guest, GuestPageStream, PageStream,
};
use bindings::wasi::cli::environment;
use bindings::wasi::clocks::monotonic_clock;
use bindings::wasi::http::{
    client,
    types::{Fields, Method, Request, RequestOptions, Response, Scheme},
};
use serde::Deserialize;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering::Relaxed},
};
use wit_bindgen::{StreamReader, StreamWriter};

struct Component;
struct StreamState {
    cancelled: AtomicBool,
    status: AtomicU8,
}

struct StreamControl {
    state: Arc<StreamState>,
}

const STATUS_RUNNING: u8 = 0;
const STATUS_CANCELLED: u8 = 1;
const STATUS_COMPLETED: u8 = 2;

/// 500ms between demo patches - slow enough for websocket clients to visibly
/// render each frame while debugging host-side buffering.
const TICK_NS: u64 = 500_000_000;

const SYSTEM_PROMPT: &str = r#"You are PageAgent. Given a prompt describing a landing page, produce 40-60 newline-delimited JSON Patch operations that progressively build that page.

Each output line MUST be a single JSON object on its own line with exactly three string fields: op, path, value.
- `op` is one of: "add", "replace", "remove".
- `path` is an RFC 6901 JSON Pointer (e.g. "/title", "/sections/0/heading", "/sections/0/items/0").
- `value` is itself a JSON-encoded value as a string. Example: to set /title to "Hello", value must be "\"Hello\"" (a JSON string literal). To set a number, value is "42". To set an object, value is "{\"x\":1}". Never put a raw string in value — always JSON-encode it first.

Build the page progressively in this order so the receiver sees it materialize naturally:
  1. add /title (string)
  2. add /subtitle (string)
  3. add /hero/headline, /hero/description, /hero/cta (strings)
  4. add /sections (empty array first if you prefer, then sections one by one)
  5. for each /sections/[i]: add /sections/[i]/heading, /sections/[i]/body, then 2-4 entries under /sections/[i]/items/[j]
  6. add /footer (string)

Use 4-6 `replace` operations sprinkled through the stream to refine earlier values — e.g. add /title as "Untitled", later replace it with the real title; add a hero cta, later replace it with a punchier one. This refinement pattern is the whole point of streaming patches, so make it visible.

Generate 3 sections. Total operations should land between 40 and 60.

Do not wrap the response in markdown. Do not add commentary. Each line is exactly one JSON object, no other text."#;

impl GuestPageStream for StreamControl {
    fn status(&self) -> String {
        match self.state.status.load(Relaxed) {
            STATUS_RUNNING => "running",
            STATUS_CANCELLED => "cancelled",
            STATUS_COMPLETED => "completed",
            _ => "unknown",
        }
        .to_string()
    }

    fn cancel_streaming(&self) {
        self.state.cancelled.store(true, Relaxed);
        self.state.status.store(STATUS_CANCELLED, Relaxed);
    }

    fn fork(&self) -> PageStream {
        PageStream::new(StreamControl {
            state: self.state.clone(),
        })
    }
}

impl Guest for Component {
    type PageStream = StreamControl;

    async fn generate_page(prompt: String) -> (StreamReader<u8>, PageStream) {
        let (mut writer, reader) = bindings::wit_stream::new::<u8>();
        let state = Arc::new(StreamState {
            cancelled: AtomicBool::new(false),
            status: AtomicU8::new(STATUS_RUNNING),
        });
        let stream_state = state.clone();

        wit_bindgen::spawn(async move {
            let result = stream_openai_chat(&prompt, &mut writer, &stream_state.cancelled).await;
            if stream_state.cancelled.load(Relaxed) {
                stream_state.status.store(STATUS_CANCELLED, Relaxed);
            } else if let Err(err) = result {
                stream_demo_page(&prompt, Some(&err), &mut writer, &stream_state.cancelled).await;
                if stream_state.cancelled.load(Relaxed) {
                    stream_state.status.store(STATUS_CANCELLED, Relaxed);
                } else {
                    stream_state.status.store(STATUS_COMPLETED, Relaxed);
                }
            } else {
                stream_state.status.store(STATUS_COMPLETED, Relaxed);
            }
            // Writer drops at end of scope -> stream closes -> MetaJson closes
            // the websocket / sink once the final line has been forwarded.
        });

        (reader, PageStream::new(StreamControl { state }))
    }
}

async fn stream_openai_chat(
    prompt: &str,
    writer: &mut StreamWriter<u8>,
    cancelled: &AtomicBool,
) -> Result<(), String> {
    let api_key = OPENAI_API_KEY;

    let model = env("PAGE_AGENT_OPENAI_MODEL")
        .or_else(|| env("OPENAI_MODEL"))
        .unwrap_or_else(|| "gpt-4o-mini".to_string());
    let host = env("PAGE_AGENT_OPENAI_HOST").unwrap_or_else(|| "api.openai.com".to_string());
    let path = env("PAGE_AGENT_OPENAI_PATH").unwrap_or_else(|| "/v1/chat/completions".to_string());
    let scheme = match env("PAGE_AGENT_OPENAI_SCHEME")
        .unwrap_or_else(|| "https".to_string())
        .as_str()
    {
        "http" => Scheme::Http,
        _ => Scheme::Https,
    };

    let body = json!({
        "model": model,
        "stream": true,
        "messages": [
            { "role": "system", "content": SYSTEM_PROMPT },
            { "role": "user", "content": prompt },
        ],
    })
    .to_string()
    .into_bytes();

    let headers = Fields::new();
    append_header(&headers, "content-type", "application/json")?;
    append_header(&headers, "accept", "text/event-stream")?;
    append_header(&headers, "content-length", &body.len().to_string())?;
    append_header(&headers, "authorization", &format!("Bearer {api_key}"))?;

    let (mut body_tx, body_rx) = bindings::wit_stream::new::<u8>();
    let (_trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let options = RequestOptions::new();
    let (request, _request_sent) = Request::new(headers, Some(body_rx), trailers_rx, Some(options));
    request
        .set_method(&Method::Post)
        .map_err(|()| "failed to set OpenAI request method".to_string())?;
    request
        .set_scheme(Some(&scheme))
        .map_err(|()| "failed to set OpenAI request scheme".to_string())?;
    request
        .set_authority(Some(&host))
        .map_err(|()| "failed to set OpenAI request authority".to_string())?;
    request
        .set_path_with_query(Some(&path))
        .map_err(|()| "failed to set OpenAI request path".to_string())?;

    wit_bindgen::spawn(async move {
        body_tx.write_all(body).await;
        drop(body_tx);
    });

    let response = client::send(request)
        .await
        .map_err(|err| format!("OpenAI request failed before response was available: {err:?}"))?;

    let status = response.get_status_code();
    let (_response_done_tx, response_done_rx) = bindings::wit_future::new(|| Ok(()));
    let (body_rx, _trailers_rx) = Response::consume_body(response, response_done_rx);

    if !(200..300).contains(&status) {
        let body = collect_body(body_rx, 4096).await;
        return Err(format!(
            "OpenAI request returned HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }

    stream_chat_completion_sse(body_rx, writer, cancelled).await
}

async fn stream_chat_completion_sse(
    mut body_rx: StreamReader<u8>,
    writer: &mut StreamWriter<u8>,
    cancelled: &AtomicBool,
) -> Result<(), String> {
    let mut line = Vec::with_capacity(1024);
    while let Some(byte) = body_rx.next().await {
        if cancelled.load(Relaxed) {
            return Ok(());
        }
        match byte {
            b'\n' => {
                handle_sse_line(&line, writer, cancelled).await?;
                line.clear();
            }
            b'\r' => {}
            byte => line.push(byte),
        }
    }

    if !line.is_empty() && !cancelled.load(Relaxed) {
        handle_sse_line(&line, writer, cancelled).await?;
    }

    Ok(())
}

async fn handle_sse_line(
    line: &[u8],
    writer: &mut StreamWriter<u8>,
    cancelled: &AtomicBool,
) -> Result<(), String> {
    if cancelled.load(Relaxed) {
        return Ok(());
    }

    let line = std::str::from_utf8(line).map_err(|err| err.to_string())?;
    let Some(data) = line.strip_prefix("data:") else {
        return Ok(());
    };
    let data = data.trim();
    if data.is_empty() || data == "[DONE]" {
        return Ok(());
    }

    let chunk: ChatCompletionChunk =
        serde_json::from_str(data).map_err(|err| format!("invalid OpenAI SSE chunk: {err}"))?;
    for choice in chunk.choices {
        if let Some(content) = choice.delta.content {
            if cancelled.load(Relaxed) {
                return Ok(());
            }
            writer.write_all(content.into_bytes()).await;
        }
    }
    Ok(())
}

async fn stream_demo_page(
    prompt: &str,
    ai_error: Option<&str>,
    writer: &mut StreamWriter<u8>,
    cancelled: &AtomicBool,
) {
    let start_ns = monotonic_clock::now();

    // Build a landing page for an "AI streaming platform" via ~30 patches.
    // Schema mirrors what the SYSTEM_PROMPT asks the AI to produce:
    //   /title, /subtitle, /hero/{headline,description,cta}, /sections/[i]/{heading,body,items/[j]}, /footer
    // 5 `replace` ops sprinkled through the stream make the "refine" pattern
    // visible: title appears as "Untitled" then becomes the real title; hero cta
    // gets sharpened; first feature bullet gets rewritten.
    let mut edits = vec![
        // 1. Title + subtitle (with a replace-refine on title)
        patch("add", "/title", json!("Untitled").to_string()),
        patch("add", "/subtitle", json!("Coming soon").to_string()),
        patch(
            "replace",
            "/title",
            json!("Streamline — patches that paint themselves").to_string(),
        ),
        patch(
            "replace",
            "/subtitle",
            json!("Real-time document streaming for AI-native apps").to_string(),
        ),

        // 2. Hero block (with cta refine)
        patch(
            "add",
            "/hero/headline",
            json!("Watch your AI's output land, one operation at a time.").to_string(),
        ),
        patch(
            "add",
            "/hero/description",
            json!("Streamline turns LLM token streams into structured JSON patches over WebSockets, so your UI updates as the model thinks.").to_string(),
        ),
        patch("add", "/hero/cta", json!("Try the demo").to_string()),
        patch(
            "replace",
            "/hero/cta",
            json!("Start streaming in 30 seconds").to_string(),
        ),

        // 3. Section 0 — Features (with refine on first bullet)
        patch("add", "/sections/0/heading", json!("Features").to_string()),
        patch(
            "add",
            "/sections/0/body",
            json!("Built for the streaming-first era.").to_string(),
        ),
        patch(
            "add",
            "/sections/0/items/0",
            json!("Patches over WebSockets").to_string(),
        ),
        patch(
            "add",
            "/sections/0/items/1",
            json!("Apply RFC 6902 ops as they arrive").to_string(),
        ),
        patch(
            "add",
            "/sections/0/items/2",
            json!("Zero buffering between AI and UI").to_string(),
        ),
        patch(
            "replace",
            "/sections/0/items/0",
            json!("Token-by-token JSON Patches over WebSockets").to_string(),
        ),

        // 4. Section 1 — Architecture
        patch(
            "add",
            "/sections/1/heading",
            json!("How it works").to_string(),
        ),
        patch(
            "add",
            "/sections/1/body",
            json!("Three wasmCloud components rendezvous through a per-workload broker. No backend stitching.").to_string(),
        ),
        patch(
            "add",
            "/sections/1/items/0",
            json!("commander — HTTP entry, fans triggers in").to_string(),
        ),
        patch(
            "add",
            "/sections/1/items/1",
            json!("page-agent — generates the patch stream").to_string(),
        ),
        patch(
            "add",
            "/sections/1/items/2",
            json!("meta-json — fans patches out over WebSocket").to_string(),
        ),

        // 5. Section 2 — Get Started
        patch(
            "add",
            "/sections/2/heading",
            json!("Get started").to_string(),
        ),
        patch(
            "add",
            "/sections/2/body",
            json!("One wash dev, one open browser tab, one prompt.").to_string(),
        ),
        patch(
            "add",
            "/sections/2/items/0",
            json!("wash build && wash dev").to_string(),
        ),
        patch(
            "add",
            "/sections/2/items/1",
            json!("open ui.html").to_string(),
        ),
        patch(
            "add",
            "/sections/2/items/2",
            json!("Type a prompt → watch your page materialize").to_string(),
        ),

        // 6. Footer + meta
        patch(
            "add",
            "/footer",
            json!("Built on wasmCloud · wasip3 streams · WebSockets").to_string(),
        ),
        patch(
            "add",
            "/meta",
            json!({"emitted_by": "page-agent", "mode": "demo-fallback", "prompt": prompt}).to_string(),
        ),
    ];

    if let Some(ai_error) = ai_error {
        edits.push(patch("add", "/meta/ai_error", json!(ai_error).to_string()));
    }

    for edit in edits {
        if cancelled.load(Relaxed) {
            break;
        }
        let elapsed_ms = monotonic_clock::now().saturating_sub(start_ns) / 1_000_000;
        let mut line = format!("[t+{:>4}ms] ", elapsed_ms).into_bytes();
        line.extend_from_slice(edit.as_bytes());
        line.push(b'\n');
        writer.write_all(line).await;
        monotonic_clock::wait_for(TICK_NS).await;
    }
}

fn patch(op: &str, path: &str, value: String) -> String {
    json!({
        "op": op,
        "path": path,
        "value": value,
    })
    .to_string()
}

async fn collect_body(mut body_rx: StreamReader<u8>, max: usize) -> Vec<u8> {
    let mut body = Vec::new();
    while let Some(byte) = body_rx.next().await {
        if body.len() >= max {
            break;
        }
        body.push(byte);
    }
    body
}

fn append_header(headers: &Fields, name: &str, value: &str) -> Result<(), String> {
    headers
        .append(&name.to_string(), &value.as_bytes().to_vec())
        .map_err(|err| format!("failed to append header {name}: {err:?}"))
}

fn env(name: &str) -> Option<String> {
    environment::get_environment()
        .into_iter()
        .find_map(|(key, value)| if key == name { Some(value) } else { None })
        .filter(|value| !value.trim().is_empty())
}

#[derive(Deserialize)]
struct ChatCompletionChunk {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    delta: ChatDelta,
}

#[derive(Deserialize)]
struct ChatDelta {
    content: Option<String>,
}

bindings::export!(Component with_types_in bindings);
