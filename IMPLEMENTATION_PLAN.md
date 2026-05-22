# bilive-rec Implementation Plan

This document is the execution plan for building `bilive-rec` from scratch.
It is intended for a local coding agent to follow in small, verifiable phases.

## Project Philosophy

`bilive-rec` is a Rust single-binary tool for recording Bilibili live streams
and uploading the result to Bilibili submissions.

The project has two governing ideas:

- Sufficiency: implement only what is required for Bilibili live recording and
  Bilibili upload.
- Radical modernization: use typed Rust APIs, explicit state transitions,
  durable local state, and clean module boundaries instead of script glue.

Additional engineering philosophy:

- Sufficiency does not mean accepting weak designs. It means implementing
  exactly the necessary design, no less and no more.
- The project does not compromise on correctness, state durability, explicit
  boundaries, or long-term maintainability.
- Large refactors are acceptable when they are necessary to preserve the design.
- Avoid small patches that preserve a wrong abstraction.
- A simple design is preferred only when it is also the correct design.
- Do not imitate Biliup's architecture. Use Biliup only as protocol reference
  material when explicitly allowed by this plan.

## Non-Goals

Version 1 must not grow these features:

- No Python.
- No Web UI, Tauri app, or Next.js frontend.
- No plugin system.
- No multi-platform live site abstraction.
- No SQLite, ORM, or server management backend.
- No ffmpeg as the primary recorder.
- No direct exposure of BiliupR types outside the uploader adapter.
- No danmaku recording in V1. Leave interface room, but do not implement it.

## Technology Stack

- Language: Rust 2024
- Runtime: Tokio
- CLI: clap
- HTTP: reqwest
- Config: serde + toml
- State: redb
- Logging: tracing + tracing-subscriber
- Errors: thiserror
- Time: jiff
- IDs: uuid
- Upload: Biliup Rust crate behind an adapter
- Recorder: custom Rust FLV recorder
- ffmpeg: optional helper for remux or fallback only

## Desired Module Layout

```text
src/
  main.rs
  cli.rs
  config.rs
  error.rs

  bilibili/
    mod.rs
    client.rs
    wbi.rs
    room.rs
    stream.rs
    cdn.rs
    types.rs

  recorder/
    mod.rs
    flv.rs
    hls.rs
    segment.rs
    remux.rs

  uploader/
    mod.rs
    biliup_adapter.rs
    types.rs

  state/
    mod.rs
    store.rs
    model.rs
    recovery.rs

  pipeline/
    mod.rs
    supervisor.rs
    session.rs
    state_machine.rs
```

## Boundary Rules

The local coding agent must follow these rules:

- Implement only the assigned phase or assigned fix.
- Keep every phase compiling.
- Run formatting, linting, and tests before reporting completion.
- Do not import or copy large parts of Biliup.
- Do not copy Biliup file structure, plugin architecture, server architecture,
  Python bridge, or UI concepts.
- If borrowing a small algorithm from Biliup is necessary later, isolate it,
  explain why, and keep the public shape aligned with this project instead of
  Biliup.
- Keep BiliupR usage inside `src/uploader/biliup_adapter.rs`.
- Keep core domain models independent from BiliupR models.
- Prefer small typed structs and enums over unstructured maps.
- Prefer explicit state transitions over implicit boolean flags.
- Do not add future-phase code "just in case".
- Do not add a dependency unless the current phase needs it.
- Do not create global mutable state.
- Do not touch unrelated modules while fixing a bounded issue.
- Do not commit phase work until the user explicitly says `ACCEPT`.

## Agent Workflow

This project is implemented by a local coding agent in small reviewed phases.

Workflow:

1. The user starts a phase with a minimal prompt naming the phase.
2. The agent must read `IMPLEMENTATION_PLAN.md` and follow the assigned phase
   scope.
3. The agent implements only the assigned phase or requested fix.
4. The agent reports using the standard Phase Report format.
5. The user and reviewer inspect the code.
6. If changes are needed, the user sends a minimal fix prompt.
7. Only after explicit `ACCEPT` may the agent commit and proceed.

A phase is not complete until it is explicitly accepted.

### Standard Phase Prompt

```text
Implement Phase X: <phase name> from `IMPLEMENTATION_PLAN.md`.

Follow the scope, allowed files, out-of-scope rules, and acceptance criteria in `IMPLEMENTATION_PLAN.md`.
Do not implement future phases.

When done, provide the Phase Report:

Implemented:
Auto-verified:
Needs user validation:
Known limitations:
Next step:
```

### Standard Fix Prompt

```text
Fix <specific issue> in <phase or slice>.

Follow the original phase scope in `IMPLEMENTATION_PLAN.md`.
Do not implement future phases.
Do not touch unrelated modules.

When done, provide the Phase Report:

Implemented:
Auto-verified:
Needs user validation:
Known limitations:
Next step:
```

### Review Verdicts

The reviewer may return one of:

- `ACCEPT`: the phase or fix is accepted and may be committed.
- `NEEDS FIX`: the implementation is close but requires a bounded follow-up.
- `DESIGN BLOCKER`: the implementation violates project philosophy, phase
  boundaries, or long-term design integrity and may require redesign.

Do not proceed to the next phase until the current phase has been explicitly
accepted.

### Phase Report Format

Every phase or fix report must use this format:

```text
Implemented:

Auto-verified:

Needs user validation:

Known limitations:

Next step:
```

Rules for the report:

- `Implemented` should state what changed at a high level.
- `Auto-verified` must list commands actually run, such as:
  - `cargo fmt`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test`
- `Needs user validation` should be honest. Do not write "none" if live API,
  real upload, or real recording behavior still needs manual validation.
- `Known limitations` must include intentional scope limits.
- `Next step` must name the next planned phase or the recommended fix.

## Small-Model Execution Contract

The local agent is expected to be literal and conservative.

For each task:

- First list the exact files it plans to change.
- Then implement only those files.
- If it discovers a missing design decision, stop and ask instead of inventing
  a large subsystem.
- Keep public types minimal and serializable when they cross module boundaries.
- Prefer compile-time stubs over speculative behavior.
- Add tests for pure logic and state persistence.
- Treat allowed-file lists as hard boundaries unless compilation requires a
  tiny import-only adjustment.
- Do not broaden a phase because the next phase seems obvious.
- Do not turn implementation phases into architecture rewrites unless the
  current design is wrong and the user has accepted the need for a redesign.

## Reference Project Protocol

The Biliup checkout in `.temp/biliup` is reference material only. It is not the
source tree for this project.

Before implementing a phase that touches Bilibili source resolution, recording,
or upload, the local agent should read only the relevant files listed below and
write a short "reference notes" section in its completion report.

Important rule: reading reference code is allowed; copying architecture is not.
The goal is to understand protocol details and edge cases, then implement a new
design in this project's structure.

### Reference Files for Phase 2

Read these files only when starting Bilibili live source resolution:

```text
.temp/biliup/biliup/plugins/bilibili.py
.temp/biliup/biliup/plugins/__init__.py
.temp/biliup/biliup/common/util.py
.temp/biliup/crates/biliup/src/downloader/extractor/bilibili.rs
```

Extract only:

- Bilibili API endpoints used for room info and play info.
- Required query parameters.
- WBI signing behavior.
- Cookie/header requirements.
- CDN and quality selection details.
- Fallback behavior around FLV, HLS TS, and HLS fMP4.

Ignore:

- Python plugin registration.
- Python base classes.
- General multi-platform abstractions.
- Biliup config layout.
- Danmaku handling.

### Reference Files for Phase 3

Read these files only when starting recorder implementation:

```text
.temp/biliup/crates/biliup/src/downloader/httpflv.rs
.temp/biliup/crates/biliup/src/downloader/flv_parser.rs
.temp/biliup/crates/biliup/src/downloader/flv_writer.rs
.temp/biliup/crates/biliup/src/downloader/util.rs
.temp/biliup/crates/biliup/src/downloader/hls.rs
```

Extract only:

- How FLV headers and tags are read.
- How keyframe-based segmentation works.
- Which metadata and sequence headers must be preserved across segments.
- How `.part` files are finalized.

Ignore:

- Biliup downloader traits.
- Server event plumbing.
- Python callback integration.
- Non-Bilibili site support.
- Bilibili network source resolution.
- Upload integration.

### Reference Files for Phase 4

Read these files only when starting upload adapter work:

```text
.temp/biliup/crates/biliup/src/uploader/bilibili.rs
.temp/biliup/crates/biliup/src/uploader/credential.rs
.temp/biliup/crates/biliup/src/uploader/line.rs
.temp/biliup/crates/biliup/src/uploader/line/upos.rs
.temp/biliup/crates/biliup-cli/src/uploader.rs
```

Extract only:

- Cookie login and token refresh flow.
- UPOS pre-upload and chunk upload flow.
- The returned Bilibili filename model.
- Submit API options and required submission fields.

Ignore:

- Biliup CLI UX.
- Dialoguer prompts.
- Progress bar implementation.
- Checkpoint format from Biliup.
- Server upload actor design.

## Core Domain Model

The exact fields can evolve during implementation, but the conceptual model
should stay stable.

```text
Room
  configured name/url
  room_id
  real_room_id
  anchor_mid

LiveSession
  session_id
  room_id
  title
  started_at
  status

StreamCandidate
  url
  protocol: Flv | HlsTs | HlsFmp4
  qn
  cdn
  headers
  lease_created_at

Segment
  session_id
  index
  path
  status: Recording | Finalized | Filtered | Uploading | Uploaded | Failed

UploadedPart
  segment_id
  bili_filename
  part_title

Submission
  session_id
  status: Pending | Submitted | Failed
  aid/bvid optional
```

## State Machine

```text
Idle
  -> Resolving
  -> Offline
  -> Recording
  -> ReResolving
  -> Uploading
  -> Submitting
  -> Submitted
  -> Failed
```

Important lifecycle rules:

- Only finalized segments may be uploaded.
- Recording writes to `*.part`; finalized files are renamed atomically.
- Each finalized segment must be persisted before upload starts.
- Upload success must be persisted before submit starts.
- Submission must be persisted with enough information to inspect later.
- Crash recovery derives the next action from redb state, not from logs.

## redb State Design

redb stores state only. It must not store video bytes.

Tables:

```text
meta
  schema_version -> u32

rooms
  room_key -> RoomState

sessions
  session_id -> LiveSession

segments
  session_id:index -> Segment

uploads
  session_id:index -> UploadedPart

submissions
  session_id -> Submission
```

V1 values should be encoded as JSON bytes with `serde_json`. This keeps early
debugging and migration easy. Binary encoding can come later if needed.

redb has a synchronous API. The async pipeline should not directly manipulate
transactions everywhere. Encapsulate it behind a `StateStore`.

## Bilibili Live Source Strategy

This is the heart of the project.

The source resolver must treat live stream URLs as short-lived leases, not
permanent addresses.

Resolution flow:

1. Accept `live.bilibili.com/{id}` and `b23.tv` live redirects.
2. Resolve short room ID to real room ID.
3. Fetch title, cover, live status, anchor uid, `live_start_time`, and
   `special_type`.
4. Refresh or cache WBI keys.
5. Call `getRoomPlayInfo` with typed parameters.
6. Build `StreamCandidate` values.
7. Select the best candidate by configured policy.
8. Health-check the selected candidate.
9. On recorder failure such as 403, timeout, invalid header, or broken stream,
   re-resolve instead of blindly retrying the old URL.

Default V1 selection policy:

- Prefer FLV.
- Prefer AVC.
- Default `qn = 10000`.
- Respect configured CDN order when present.
- Fall back to another CDN when health checks fail.
- Keep HLS/fMP4 as future or fallback paths, not the V1 default.

## Recorder Strategy

V1 primary path:

- Custom Rust FLV recorder.
- Write `*.part` while recording.
- Rename to final `*.flv` only after the segment is complete.
- Segment by keyframe.
- Preserve or inject required FLV metadata and codec sequence headers.
- Support segmenting by time and by file size.
- Mark files under a configured minimum size as `Filtered`.

ffmpeg role:

- Optional remux helper, such as FLV to MP4.
- Optional special-case fallback.
- Not part of the default recording control flow.

## Upload Strategy

Upload uses BiliupR through an adapter.

Define a project-owned trait similar to:

```text
Uploader
  check_login()
  upload_segment(path) -> UploadedPart
  submit(session, uploaded_parts) -> SubmissionResult
```

The adapter may use BiliupR internally for:

- Cookie JSON login.
- UPOS pre-upload.
- Chunk upload.
- Bilibili `filename` return value.
- App/Web submission.
- Cover upload later.

The rest of the project must only use project-owned types.

## CLI Surface

```bash
bilive-rec login
bilive-rec check <room-url>
bilive-rec record <room-url>
bilive-rec upload <file...>
bilive-rec run --config config.toml
bilive-rec state inspect
bilive-rec state recover
```

The most important V1 command is:

```bash
bilive-rec run --config config.toml
```

## Config Shape

Initial config target:

```toml
[data]
dir = "./data"

[record]
output_dir = "./recordings"
segment_time = "01:00:00"
segment_size = "2GiB"
min_segment_size = "20MiB"
prefer_protocol = "flv"
qn = 10000
cdn = []

[upload]
cookie_file = "./data/cookies.json"
line = "auto"
threads = 3
submit_api = "app"
tid = 171
copyright = 2
tags = ["直播录像"]

[[rooms]]
name = "example"
url = "https://live.bilibili.com/123456"
title = "{streamer} {title} {date}"
description = "{streamer} 直播录像\n原直播间：{url}"
```

## Recovery Rules

At startup or through `state recover`, apply these rules:

```text
Recording with only .part file -> mark interrupted
Finalized but not Uploaded -> upload
Uploaded but not Submitted -> submit when session is complete
Submitting with unknown result -> report for inspection in V1
Failed -> preserve error, allow explicit retry later
```

Do not silently delete user data during recovery.

## Phase 0: Project Skeleton

Goal: create the Rust project and module skeleton without business behavior.

Allowed files:

```text
Cargo.toml
src/main.rs
src/cli.rs
src/config.rs
src/error.rs
src/bilibili/mod.rs
src/bilibili/types.rs
src/recorder/mod.rs
src/recorder/segment.rs
src/uploader/mod.rs
src/uploader/types.rs
src/state/mod.rs
src/state/model.rs
src/state/store.rs
src/state/recovery.rs
src/pipeline/mod.rs
src/pipeline/state_machine.rs
```

Tasks:

- Initialize Cargo package.
- Use Rust edition 2024.
- Add only these baseline dependencies:

```toml
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "fs", "process", "time"] }
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.9"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
redb = "2"
jiff = { version = "0.2", features = ["serde"] }
uuid = { version = "1", features = ["v4", "serde"] }
```

- Create module layout.
- Add CLI stubs only. No real Bilibili network calls, recording, or upload.
- Supported CLI variants for stubs:

```text
login
check <room-url>
record <room-url>
upload <file...>
run --config <path>
state inspect
state recover
```

- The CLI may print "not implemented" for future phases.
- Add `AppResult<T> = Result<T, AppError>`.
- Add an `AppError` enum with only broad variants needed for stubs.
- Add empty domain type placeholders only if needed for compile.

Out of scope:

- No reqwest dependency yet.
- No BiliupR dependency yet.
- No recorder implementation.
- No redb table implementation beyond type placeholders.
- No config file parsing behavior beyond function signatures if needed.

Acceptance:

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

## Phase 1: Config, Errors, Logging, redb State

Goal: build the foundation every later phase depends on.

Allowed files:

```text
Cargo.toml
src/main.rs
src/cli.rs
src/config.rs
src/error.rs
src/state/mod.rs
src/state/model.rs
src/state/store.rs
src/state/recovery.rs
src/pipeline/state_machine.rs
```

Do not edit Bilibili resolver, recorder, or uploader modules in this phase
unless compilation requires a tiny import adjustment.

Tasks:

- Define `AppConfig`.
- Load config from TOML.
- Define `AppError`.
- Initialize `tracing`.
- Define state models.
- Implement `StateStore`.
- Create/open redb database under configured data directory.
- Add schema version metadata.
- Write/read sessions and segments.
- Add simple `state inspect` command that prints table counts or a minimal
  JSON summary.
- Add simple `state recover` command that currently performs no mutation and
  reports "no recovery actions implemented yet".

Concrete config model:

```text
AppConfig
  data: DataConfig
  record: RecordConfig
  upload: UploadConfig
  rooms: Vec<RoomConfig>

DataConfig
  dir: PathBuf

RecordConfig
  output_dir: PathBuf
  segment_time: Option<String>
  segment_size: Option<String>
  min_segment_size: String
  prefer_protocol: PreferredProtocol
  qn: u32
  cdn: Vec<String>

UploadConfig
  cookie_file: PathBuf
  line: String
  threads: usize
  submit_api: SubmitApi
  tid: u16
  copyright: u8
  tags: Vec<String>

RoomConfig
  name: String
  url: String
  title: Option<String>
  description: Option<String>
```

Concrete state model:

```text
LiveSession
  id: Uuid
  room_key: String
  title: String
  started_at: jiff timestamp type
  status: SessionStatus

Segment
  session_id: Uuid
  index: u32
  path: PathBuf
  status: SegmentStatus
  error: Option<String>

UploadedPart
  session_id: Uuid
  segment_index: u32
  bili_filename: String
  part_title: String

Submission
  session_id: Uuid
  status: SubmissionStatus
  aid: Option<u64>
  bvid: Option<String>
  error: Option<String>
```

Concrete redb API target:

```text
StateStore::open(path: impl AsRef<Path>) -> AppResult<Self>
StateStore::init_schema(&self) -> AppResult<()>
StateStore::schema_version(&self) -> AppResult<u32>
StateStore::put_session(&self, session: &LiveSession) -> AppResult<()>
StateStore::get_session(&self, id: Uuid) -> AppResult<Option<LiveSession>>
StateStore::put_segment(&self, segment: &Segment) -> AppResult<()>
StateStore::list_segments(&self, session_id: Uuid) -> AppResult<Vec<Segment>>
StateStore::summary(&self) -> AppResult<StateSummary>
```

Encoding:

- redb keys should be simple strings or bytes.
- redb values should be `serde_json` bytes.
- Keep table definitions private to `state::store`.

Acceptance:

- Database can be created.
- State can be written and read.
- Reopening the app preserves state.
- Tests cover basic store operations.
- Config loading has a test with an inline TOML string.
- State enums serialize and deserialize.

Required verification:

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

## Phase 2: Bilibili Live Resolver

Goal: make `bilive-rec check <url>` report live status and candidate streams.

Do not delegate all of Phase 2 at once to a small model. Use these slices.

### Phase 2A: Typed HTTP Client and WBI

Allowed files:

```text
Cargo.toml
src/bilibili/client.rs
src/bilibili/wbi.rs
src/bilibili/types.rs
src/error.rs
```

Tasks:

- Implement typed `BiliClient`.
- Implement WBI key fetching and signing.
- Add `reqwest` dependency only in this slice.
- Add unit tests for WBI signing with fixed keys and fixed timestamp.

Out of scope:

- No room status logic.
- No stream candidate selection.
- No CLI behavior beyond compile fixes.

### Phase 2B: Room Resolution

Allowed files:

```text
src/bilibili/room.rs
src/bilibili/types.rs
src/cli.rs
src/main.rs
```

Tasks:

- Implement room resolution.
- Implement live status fetch.
- Support `live.bilibili.com/{id}` first.
- Add `b23.tv` redirect support only if it stays small.
- Capture `special_type` from the room-info response.

Out of scope:

- No stream candidate parsing.
- No recorder integration.

### Phase 2C: Stream Candidates and CDN Policy

Allowed files:

```text
src/bilibili/stream.rs
src/bilibili/cdn.rs
src/bilibili/types.rs
src/config.rs
```

Tasks:

- Implement stream candidate generation from `getRoomPlayInfo`.
- Prefer FLV, AVC, configured qn, and configured CDN order.
- Implement basic health check using HTTP status/header only.
- Return a selected `StreamCandidate`.
- Preserve observability for failed health checks with `tracing`.

Out of scope:

- No recording.
- No HLS segment downloading.
- No advanced CDN replacement tricks.

### Phase 2D: `check` Command Output

Allowed files:

```text
src/cli.rs
src/main.rs
src/bilibili/mod.rs
```

Tasks:

- Wire `bilive-rec check <url>`.
- Print either offline status or selected stream details.
- Output must be human-readable and stable enough for debugging.

Acceptance:

```bash
bilive-rec check https://live.bilibili.com/<room>
```

Expected output must clearly show either:

```text
offline
```

or:

```text
live
title = ...
room_id = ...
candidates = [...]
selected = ...
```

## Phase 3: FLV Recorder

Goal: record a selected FLV stream into finalized segment files.

Do not delegate all of Phase 3 at once to a small model. Use these slices.

### Phase 3A: Segment Policy and File Lifecycle

Allowed files:

```text
src/recorder/segment.rs
src/recorder/mod.rs
src/state/model.rs
src/state/store.rs
```

Tasks:

- Define `SegmentPolicy`.
- Define `SegmentEvent`.
- Implement `.part` path and final path lifecycle helpers.
- Add tests for file naming and size/time threshold logic.

Expected design shape:

- `SegmentPolicy` should hold the output directory and segment thresholds.
- Path helpers should derive paths from `SegmentPolicy.output_dir`, not from a
  second base directory argument.
- `SegmentEvent` should carry enough identity for future persistence:
  - `session_id`
  - `index`
  - relevant path
  - size when finalized or filtered
- Threshold helpers should remain pure logic:
  - rotate by size
  - rotate by elapsed time
  - filter by final size

Out of scope:

- No FLV parsing.
- No network stream reading.
- No file writing beyond pure path/lifecycle helpers unless absolutely required.

### Phase 3B: Minimal FLV Reader/Writer

Allowed files:

```text
src/recorder/flv.rs
src/recorder/mod.rs
src/error.rs
```

Tasks:

- Implement FLV reader/parser enough for recording.
- Implement segment writer primitives.
- Add tests with small synthetic FLV-like bytes where practical.
- Keep the implementation local to FLV byte structure and writer behavior.

Required FLV concepts:

- FLV header validation.
- FLV tag header parsing.
- PreviousTagSize handling.
- Tag type identification.
- Minimal keyframe detection for AVC video tags.
- Preservation of metadata and codec sequence headers needed for later segment
  writing.

Out of scope:

- No Bilibili network integration.
- No HTTP stream reader.
- No `BiliClient` usage.
- No upload integration.
- No pipeline supervisor.
- No live recording loop.
- No HLS segment downloading.

### Phase 3C: Recording Loop

Allowed files:

```text
src/recorder/mod.rs
src/recorder/flv.rs
src/bilibili/types.rs
src/state/store.rs
```

Tasks:

- Write to `*.part` and rename on finalization.
- Segment by keyframe.
- Persist segment state to redb.
- Emit finalized segment events.

Out of scope:

- No pipeline supervisor.
- No upload after segment.

Acceptance:

- At least one recorded FLV file is playable.
- Finalized segments are visible in redb state.
- Invalid/failed streams return clear errors.

## Phase 4: BiliupR Upload Adapter

Goal: upload local files and submit through BiliupR without leaking BiliupR
types into the core project.

Do not delegate all of Phase 4 at once to a small model. Use these slices.

### Phase 4A: Project-Owned Uploader Types

Allowed files:

```text
src/uploader/types.rs
src/uploader/mod.rs
src/state/model.rs
```

Tasks:

- Define project-owned uploader trait and types.
- Define `UploadRequest`, `UploadedPart`, `SubmissionRequest`, and
  `SubmissionResult`.
- Keep all types independent from BiliupR.

Out of scope:

- No BiliupR dependency yet.
- No network upload.

### Phase 4B: BiliupR Adapter

Allowed files:

```text
Cargo.toml
src/uploader/biliup_adapter.rs
src/uploader/mod.rs
src/error.rs
```

Tasks:

- Add BiliupR dependency pinned to a commit or local path during development.
- Implement `BiliupUploader`.
- Translate from project-owned request types to BiliupR internal types.
- Translate BiliupR results back to project-owned types.

Out of scope:

- No pipeline integration.
- No BiliupR type exposure outside adapter.

### Phase 4C: Upload CLI

Allowed files:

```text
src/cli.rs
src/main.rs
src/uploader/mod.rs
```

Tasks:

- Implement `upload <file...>` command.
- Use the adapter through the project-owned trait.

Acceptance:

```bash
bilive-rec upload ./some.flv --title test
```

The command should either upload and submit successfully or fail with a clear,
actionable login/cookie/API error.

## Phase 5: Pipeline

Goal: connect monitor, resolver, recorder, uploader, and submission.

Do not delegate all of Phase 5 at once to a small model. Use these slices.

### Phase 5A: State Machine Only

Allowed files:

```text
src/pipeline/state_machine.rs
src/pipeline/session.rs
src/state/model.rs
```

Tasks:

- Implement state enum and transition validation.
- Add tests for allowed and disallowed transitions.

Out of scope:

- No network calls.
- No recorder or uploader orchestration.

### Phase 5B: Supervisor Skeleton

Allowed files:

```text
src/pipeline/supervisor.rs
src/pipeline/mod.rs
src/config.rs
src/state/store.rs
```

Tasks:

- Implement supervisor.
- Monitor configured rooms.
- Persist every state transition.

Out of scope:

- Keep actual resolver/recorder/uploader calls behind traits or TODO stubs if
  needed.

### Phase 5C: End-to-End Wiring

Allowed files:

```text
src/pipeline/supervisor.rs
src/main.rs
src/cli.rs
```

Tasks:

- Start recording when live.
- Upload finalized segments while recording continues.
- Submit after offline confirmation.
- Persist every state transition.

Acceptance:

- `bilive-rec run --config config.toml` can complete a full live session.
- State remains inspectable while running.
- Upload and submit can resume from persisted state.

## Phase 6: Recovery and Hardening

Goal: make failures boring.

Phase 6 focuses on making persisted state observable, recovery actions explicit
and safe, and shutdown behavior honest. Recovery must never hide data loss,
silently retry ambiguous submissions, or upload incomplete files.

### Safety Rules

- `state inspect` must be read-only.
- `state recover` must default to dry-run / plan-only behavior.
- Any mutation must require explicit user intent, such as `--apply`.
- Recovery actions must be idempotent.
- Do not auto-retry `SubmissionStatus::Pending`.
- Do not blindly retry `SubmissionStatus::Failed`.
- Do not upload incomplete `.part` files.
- Do not delete or rename user data by default.
- Do not hide `Failed` states by silently resetting them to `Idle`.
- Preserve failed states and error messages until an explicit recovery action is
  requested.
- Graceful shutdown must not pretend an incomplete recording is finalized.

### Tasks

- Improve `state inspect`.
- Implement safe `state recover` dry-run planning.
- Implement explicit recovery apply actions for safe cases.
- Handle interrupted `.part` files conservatively.
- Retry finalized but unuploaded segments only through explicit, inspectable
  recovery flow.
- Preserve failed states with error messages.
- Add signal handling for graceful shutdown.

### State Inspection

`bilive-rec state inspect` must provide a useful human-readable view of the
persisted runtime state.

It should show:

- schema version
- pipeline states by room ID
- sessions:
  - session ID
  - room key
  - title
  - started time
  - status
- segments grouped by session:
  - index
  - status
  - path
  - error if present
- uploaded parts grouped by session:
  - segment index
  - Bilibili filename
  - part title
- submissions:
  - session ID
  - status
  - aid/bvid if present
  - error if present
- detected anomalies and suggested recovery actions

Examples of anomalies:

- `SegmentStatus::Recording` left behind by crash
- `.part` files referenced by interrupted or failed segments
- finalized segments missing `UploadedPart`
- pending submissions
- failed submissions
- rooms stuck in failed pipeline state

`state inspect` must not mutate redb, touch files, call Bilibili APIs, upload,
or submit.

### Recovery Dry Run

`bilive-rec state recover` without mutation flags must be dry-run only.

It should print planned actions, such as:

```text
Would mark segment <session>/<index> as Failed: Interrupted by hard crash
Would leave pending submission <session> unchanged: requires manual verification
Would leave failed pipeline state for room <room_id> unchanged: use explicit reset
````

Default recovery must not write to redb, rename files, delete files, upload, or
submit.

### Recovery Apply

Mutating recovery must require explicit flags.

Safe apply actions may include:

* marking interrupted `SegmentStatus::Recording` segments as
  `SegmentStatus::Failed` with an error such as `"Interrupted by hard crash"`
* leaving the referenced `.part` file on disk unchanged
* resetting a failed room pipeline state to `Idle` only through an explicit room
  target and only when the user accepts that old failure is not being retried
* moving a room back to `Uploading` only for upload reconciliation of finalized
  segments missing `UploadedPart`

Unsafe or ambiguous actions must be refused unless future remote verification is
implemented:

* automatically retrying `SubmissionStatus::Pending`
* automatically retrying ambiguous `SubmissionStatus::Failed`
* uploading incomplete `.part`
* deleting or renaming incomplete files

If a manual failed-submit retry is later added, it must require explicit
confirmation that the user has checked Bilibili and confirmed there is no remote
submission, for example:

```bash
bilive-rec state recover --retry-submit <SESSION_ID> --i-confirm-no-remote-submission
```

This action must not be implemented as an automatic default.

### Graceful Shutdown

Graceful shutdown should be implemented only after inspect and recovery behavior
is useful enough to diagnose interrupted state.

On Ctrl-C or shutdown:

* notify room supervisors to stop
* do not blindly rename active `.part` files to `.flv`
* only finalize a segment if the recorder can prove it is complete and valid
* otherwise leave incomplete `.part` files untouched and persist an inspectable
  failed/interrupted state
* do not upload incomplete segments
* do not transition to `Idle` in a way that hides unfinished upload, submit, or
  failed state

### Acceptance

```bash
bilive-rec state inspect
bilive-rec state recover
```

Both commands must be useful and safe.

Specific acceptance requirements:

* `state inspect` can explain stuck Recording, Uploading, Submitting, and Failed
  states without opening redb manually.
* `state inspect` is read-only.
* `state recover` without flags is dry-run only.
* `state recover --apply` mutates only explicitly safe and idempotent state.
* interrupted `.part` files are not uploaded, deleted, or blindly finalized.
* pending submissions are not retried automatically.
* failed submissions are not retried automatically.
* failed pipeline states are not silently hidden.
* graceful shutdown does not corrupt segment state or misrepresent incomplete
  files as finalized recordings.
