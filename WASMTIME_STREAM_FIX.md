# Wasmtime Cross-Component Stream Fix Notes

This note is the handoff for fixing the remaining `patch-stream` bug in a local
wasmtime checkout, wiring wasmCloud to that checkout, and using the
`examples/patch-stream` repro as proof before opening an upstream wasmtime PR.

Local paths used here:

- wasmCloud: `/home/aditya-sal/Desktop/wasmCloud`
- wasmtime: `/home/aditya-sal/Desktop/wasmtime`

The local wasmtime checkout has already been fast-forwarded to
`upstream/main` at:

```text
f37fcc9b0c Reimplement passive data segments (#13394)
```

At that revision, wasmtime's workspace version is `46.0.0`, so wasmCloud's
workspace dependency versions must be adjusted while testing against the local
checkout.

## Repro Symptom

Run from wasmCloud:

```sh
cd /home/aditya-sal/Desktop/wasmCloud
cargo build -p wash --features wasip3
cd examples/patch-stream
cargo +nightly build --workspace --target wasm32-wasip2 --release
../../target/debug/wash dev
```

Then in another shell:

```sh
curl -sS http://127.0.0.1:8000/
```

Current bad behavior:

- `meta-json` receives a `stream<u8>` handle and starts draining.
- The payload bytes are not the producer's NDJSON lines.
- The output looks like copied component memory / ABI structs.
- A consumer-side local drain of the producer stream can panic in wasmtime with:

```text
BUG: expected write payload type to be present
```

Trace showed the same stream handle crossing components, but with different
guest stream type slots:

```text
subscribe results_buf=[Stream(StreamAny { id: TransmitHandle(10), ty: Guest(StreamType(... index: TypeStreamIndex(1))) })]
send-stream params=[Stream(StreamAny { id: TransmitHandle(10), ty: Guest(StreamType(... index: TypeStreamIndex(0))) })]
```

That makes this a wasmtime stream-copy/type-table problem, not a producer,
consumer, or `meta-json` bug.

## Likely Upstream Bug

Patch target in wasmtime:

```text
/home/aditya-sal/Desktop/wasmtime/crates/wasmtime/src/runtime/component/concurrent/futures_and_streams.rs
```

Focus on `Instance::copy`, currently around line 3271 on `upstream/main`.

The suspicious pattern is:

```rust
let (component, mut store) = self.component_and_store_mut(store.0);
let types = component.types();

let write_payload_ty = write_ty.payload(types);
...
let read_payload_ty = read_ty.payload(types);
```

`write_ty` belongs to the writer component instance and `read_ty` belongs to
the reader component instance. Under dynamic linking in wash, those are
separate `Component`s with separate `ComponentTypes` tables. Resolving both
indices against `self.component().types()` only works when both ends share one
composed component graph.

The `copy` function already receives:

- `write_caller_instance`
- `write_ty`
- `read_caller_instance`
- `read_ty`

But the states around it also carry the concrete wasmtime `Instance`:

- `WriteState::GuestReady { instance, ... }`
- `ReadState::GuestReady { instance, ... }`

The fix should use the writer instance's component types for writer payload ABI
and the reader instance's component types for reader payload ABI.

## Patch Shape

High-level change:

1. Change `Instance::copy` to accept both endpoint instances, for example:

```rust
fn copy<T: 'static>(
    self,
    store: StoreContextMut<T>,
    flat_abi: Option<FlatAbi>,
    write_instance: Instance,
    write_caller_instance: RuntimeComponentInstanceIndex,
    write_ty: TransmitIndex,
    write_options: OptionsIndex,
    write_address: usize,
    read_instance: Instance,
    read_caller_instance: RuntimeComponentInstanceIndex,
    read_caller_thread: QualifiedThreadId,
    read_ty: TransmitIndex,
    read_options: OptionsIndex,
    read_address: usize,
    count: ItemCount,
    rep: u32,
) -> Result<()>
```

2. Resolve type metadata separately:

```rust
let (read_component, mut store) = read_instance.component_and_store_mut(store.0);
let read_types = read_component.types();

// Add a small helper if borrow-checking gets tight. The important point is
// that this comes from `write_instance`, not `self` or `read_instance`.
let write_component = write_instance.id().get(store).component();
let write_types = write_component.types();

let write_payload_ty = write_ty.payload(write_types);
let write_abi = match write_payload_ty {
    Some(ty) => write_types.canonical_abi(ty),
    None => &CanonicalAbiInfo::ZERO,
};

let read_payload_ty = read_ty.payload(read_types);
let read_abi = match read_payload_ty {
    Some(ty) => read_types.canonical_abi(ty),
    None => &CanonicalAbiInfo::ZERO,
};
```

3. Use the right type tables when loading and storing:

```rust
let lift = &mut LiftContext::new(store_opaque, write_options, write_instance);
Val::load(lift, *write_payload_ty, bytes)?;

let lower = &mut LowerContext::new(store.as_context_mut(), read_options, read_instance);
value.store(lower, *read_payload_ty, ptr)?;
```

4. Update both call sites:

- In `guest_write`, when matching `ReadState::GuestReady { instance:
  read_instance, ... }`, call `self.copy(..., self, ..., read_instance, ...)`.
- In `guest_read`, stop discarding `WriteState::GuestReady { instance: _, ... }`;
  bind it as `write_instance` and call `self.copy(..., write_instance, ..., self,
  ...)`.

5. Re-check later calculations that still use `ty.payload(types)` after `copy`.
   Those should use the local side's component types:

- writer-side remaining-buffer math should use the writer instance/types.
- reader-side remaining-buffer math should use the reader instance/types.

This is the part to be careful with. The corruption we saw comes from payload
type table confusion, and there are a few sibling calculations nearby.

## Wasmtime Local Checks

From the wasmtime checkout:

```sh
cd /home/aditya-sal/Desktop/wasmtime
git switch -c fix-cross-component-stream-copy
cargo fmt
cargo check -p wasmtime --features component-model,component-model-async
```

If the compile passes, run a narrower test next. Useful starting points:

```sh
cargo test -p wasmtime --features component-model,component-model-async --test all
cargo test -p wasmtime --features component-model,component-model-async component_model
```

The exact upstream regression test should ideally live in wasmtime, not
wasmCloud. Search the existing async component-model tests first:

```sh
rg -n "stream|future|component-model/async|func_new_concurrent|run_concurrent" \
  tests crates/wasmtime/tests crates/wasmtime/src/runtime/component
```

The test should prove a stream created by one component can be read by another
component when the two components have independent type tables.

## Point wasmCloud At Local Wasmtime

Because `/home/aditya-sal/Desktop/wasmtime` is now workspace version `46.0.0`,
temporarily bump the wasmtime family in wasmCloud's root `Cargo.toml` from `44`
to `46` while testing:

```toml
wasmtime = { version = "46", default-features = false }
wasmtime-wasi = { version = "46", default-features = false }
wasmtime-wasi-io = { version = "46", default-features = false }
wasmtime-wasi-http = { version = "46", default-features = false }
wasmtime-wasi-tls = { version = "46", default-features = false }
```

Then add temporary local path overrides under `[patch.crates-io]`:

```toml
wasmtime = { path = "../wasmtime/crates/wasmtime" }
wasmtime-wasi = { path = "../wasmtime/crates/wasi" }
wasmtime-wasi-io = { path = "../wasmtime/crates/wasi-io" }
wasmtime-wasi-http = { path = "../wasmtime/crates/wasi-http" }
wasmtime-wasi-tls = { path = "../wasmtime/crates/wasi-tls" }
```

Do not include these local path overrides in a wasmCloud PR. They are only for
proving the local wasmtime fix against this repro.

Then from wasmCloud:

```sh
cd /home/aditya-sal/Desktop/wasmCloud
cargo update -p wasmtime -p wasmtime-wasi -p wasmtime-wasi-io -p wasmtime-wasi-http -p wasmtime-wasi-tls
cargo build -p wash --features wasip3
```

If the 44 -> 46 bump exposes wasmCloud API drift, either fix the small compile
breaks locally or create a wasmtime branch based on `upstream/release-44.0.0`
for the wasmCloud repro. The upstream wasmtime PR should still target
`upstream/main`.

## wasmCloud Verification

Run the repro again:

```sh
cd /home/aditya-sal/Desktop/wasmCloud/examples/patch-stream
cargo +nightly build --workspace --target wasm32-wasip2 --release
../../target/debug/wash dev
```

In another shell:

```sh
curl -sS http://127.0.0.1:8000/
```

Passing behavior:

- no panic from wasmtime
- no corrupted binary-looking `meta-json` lines
- `meta-json` logs the producer's expected NDJSON lines, for example:

```text
meta-json: [  1] [t+   0ms] {"op":"add","path":"/title","value":"\"Untitled\""}
meta-json: [  2] [t+ 120ms] {"op":"add","path":"/version","value":"0"}
```

Optional trace run:

```sh
RUST_LOG=wash_runtime::engine::workload=trace ../../target/debug/wash dev
```

The stream handle may still show different local type indices on different
components, but the data copy must use the correct writer and reader component
type tables internally.

## Upstream PR Checklist

For the wasmtime PR:

- Add a regression test in wasmtime that fails before the fix and passes after.
- Keep the fix scoped to concurrent component `{future,stream}` copy logic.
- Explain that dynamically linked components can have distinct `ComponentTypes`
  tables, so `write_ty` and `read_ty` cannot both be resolved through one
  endpoint's `component.types()`.
- Include the wasmCloud `examples/patch-stream` repro as external validation in
  the PR description, but keep the upstream test self-contained in wasmtime.

