# wsfs — Core Specification (MVP)

## Purpose

Browser-native virtual filesystem that:
- Caches reads/writes locally (IndexedDB in browsers, in-memory fallback elsewhere)
- Syncs in batches with optimistic concurrency (`If-Match` + ETags)
- Surfaces conflicts and attribution (`updatedBy`)
- Supports pluggable codecs for encrypting/compressing payloads

## Architecture

1. Local Store (IndexedDB or in-memory)
2. Sync Engine (batch push/pull + conflict detection)
3. Remote Backend (REST-ish API implemented via `createWsfsApi`)

## Client API

```ts
import { Wsfs } from "@mfukala/wsfs/client";

const wsfs = await Wsfs.init({
  namespace: "vault",
  backendUrl: "https://example.com/api/wsfs",
});

// Writes run inside a write task (exclusive lock + rollback if the task throws).
await wsfs.runWriteTask(async (fs) => {
  await fs.write("/primary/item1.json", JSON.stringify({ hello: "world" }));
});

// Reads run in read tasks (may run concurrently).
const data = await wsfs.runReadTask((fs) => fs.read("/primary/item1.json"));

// Push local edits and pull remote updates.
await wsfs.sync();
```

- `runWriteTaskAndSync(task, prefix?)` keeps the write lock through sync; local edits stay even if sync fails.
- `list(prefix?)` returns `{ path, etag, encoding?, updatedBy? }[]` without local tombstones.
- `info(path)` returns `{ etag, encoding, updatedBy? }` using cached metadata when available.
- `readMany(paths)` / `infoMany(paths)` fetch multiple files/metadata entries in one round-trip, filling the local cache for offline use; missing entries resolve to `null`.
- Conflicts emit a `CustomEvent<ConflictEventDetail>` on the `wsfs` instance (`detail` includes `path`, `localEtag`, `remoteEtag`, `updatedBy?`).
- Optional `codec` lets callers transform payloads before storage/network; defaults to pass-through.
- Optional `attachAuth(kind, payload)` can inject headers or extra body fields before each request (e.g., a signed `proof` field); the server’s `authorize` hook sees these fields.

## Sync Behavior

- Dirty local writes/deletes are sent with their last-known `etag` (or `"*"` for creates).
- Remote deltas are detected using the caller’s `known` set; incremental sync uses a `watermark` when the backend supplies one.
- Conflicts occur when remote `etag` differs; the client keeps local state and emits an event so callers can resolve.
- Binary payloads travel as `contentBase64` with `encoding: "base64"`; UTF-8 uses `content` + `encoding: "utf8"`.

## Backend REST API (as expected by the client)

- `POST /sync` — batch push/pull (see shapes below)
- `GET /file?path=/foo.txt` → `{ etag, encoding, updatedBy?, content|contentBase64 }`
- `GET /file/info?path=/foo.txt` → `{ etag, encoding, updatedBy? }`
- `POST /file/batch` + `{ paths: [string, ...] }` → `[ { etag, encoding, updatedBy?, content|contentBase64 } | null, ... ]`
- `POST /file/info/batch` + `{ paths: [string, ...] }` → `[ { etag, encoding, updatedBy? } | null, ... ]`
- `GET /list?prefix=/path/` → `[{ path, etag, encoding }]`
- `PUT /file` + header `If-Match: <etag|*>` + body `{ path, content|contentBase64, encoding?, updatedBy? }` → `{ etag }`
- `DELETE /file?path=...` + header `If-Match: <etag|*>`

The bundled `MemoryPersistence` enforces `If-Match` (use `"*"` to create new files) and tracks `updatedBy`. `createWsfsApi` accepts optional hooks: `authorize(kind, payload)` may throw with `status` 401/403/400 to short-circuit a request (payload includes headers/body + any custom fields like `proof`), and `partition(ctx)` can pick a tenant for `persistence.withPartition(...)` (falls back to the adapter’s default partition when undefined).

## Sync request/response shapes

`POST /sync` request:
```json
{
  "prefix": "/path/",
  "writes": [{
    "path": "/foo.txt",
    "content": "...",
    "contentBase64": "...",       // optional alternative for binary
    "encoding": "utf8 | base64",  // defaults to utf8
    "updatedBy": "author-id",
    "ifMatch": "<etag or *>",
    "proof": "signed payload"      // extra fields are allowed
  }],
  "deletes": [{ "path": "/bar.txt", "ifMatch": "<etag or *>", "proof": "signed tombstone" }],
  "known": [{ "path": "/existing.txt", "etag": "<last-known-etag>" }],
  "watermark": "<optional incremental cursor>"
}
```

Response:
```json
{
  "writeResults": [{
    "path": "/foo.txt",
    "status": 200,
    "etag": "<new-etag>",
    "remoteEtag": "<latest-remote-etag>",
    "updatedBy": "remote-author",
    "error": "reason when status >= 400"
  }],
  "deleteResults": [{
    "path": "/bar.txt",
    "status": 204,
    "remoteEtag": "<latest-remote-etag>",
    "updatedBy": "remote-author",
    "error": "reason when status >= 400"
  }],
  "remoteUpdates": [{
    "path": "/existing.txt",
    "etag": "<etag>",
    "encoding": "utf8 | base64",
    "updatedBy": "remote-author",
    "content": "...",           // when encoding is utf8
    "contentBase64": "..."      // when encoding is base64
  }],
  "remoteMissing": ["/existing.txt"],
  "watermark": "<latest cursor or null>"
}
```
