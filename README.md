# wsfs

wsfs (Web Sync File System) is a browser-friendly, local-first virtual filesystem for apps that need offline sync, IndexedDB caching, optimistic concurrency (ETag/If-Match), and pluggable backend persistence—zero runtime deps, works with any REST-ish API.

## What it solves (at a glance)

- Offline-first reads/writes with IndexedDB caching and in-memory fallback (works in browsers, workers, and Node.js)
- Batch sync with optimistic concurrency; conflict events when remote ETags differ
- Pluggable codecs for encrypting/compressing payloads before storage or network
- Drop-in server toolkit that adapts to Express/Next.js or any framework with your own persistence adapter
- Zero runtime dependencies; ships as `@mfukala/wsfs/client` and `@mfukala/wsfs/server`

## Sample use cases

- Offline-friendly dashboards or CRMs that sync JSON blobs to an API while users travel  
  Queries: "browser offline sync indexeddb rest etag", "local first crm cache optimistic concurrency", "web app sync json files"
- Collaborative notes/tasks where conflicts should surface but not auto-merge  
  Queries: "conflict aware offline note app etag", "optimistic concurrency file sync web", "custom conflict event listener indexeddb"
- Self-hosted product data/settings that must work in service workers and fall back when the backend is down  
  Queries: "service worker offline filesystem", "local first config storage web", "sync files nextjs api route if-match"
- Binary or encrypted payloads that need custom codecs before storage/transfer  
  Queries: "encrypt before indexeddb storage", "compress api payload sync", "browser base64 file sync"
- Rapid prototyping of syncable file trees without picking a database upfront  
  Queries: "virtual filesystem over http", "simple rest file sync adapter", "express etag file api starter"

The package ships:

- A browser client (`@mfukala/wsfs/client`) that caches reads/writes locally, syncs in batches, and surfaces conflicts
- A server toolkit (`@mfukala/wsfs/server`) that wires the sync protocol into any persistence adapter (an in-memory adapter is included)
- No runtime dependencies

## Browser client (`@mfukala/wsfs/client`)

```ts
import { Wsfs } from "@mfukala/wsfs/client";

const wsfs = await Wsfs.init({
  namespace: "vault", // IndexedDB namespace
  backendUrl: "http://localhost:8787", // your API base URL
});

// Mutations run inside a write task (exclusive lock + rollback on task failure).
await wsfs.runWriteTask(async (fs) => {
  await fs.write("/primary/item.json", JSON.stringify({ hello: "world" }));
});

// Reads run in read tasks (can run concurrently).
const item = await wsfs.runReadTask((fs) => fs.read("/primary/item.json"));

// Batch push local changes and pull remote updates.
await wsfs.sync();
```

- `runWriteTaskAndSync` keeps the write lock through the sync; local edits rollback only if the task throws, not if sync fails.
- `list(prefix?)` hides local tombstones; `info(path)` returns `{ etag, encoding, updatedBy? }`.
- Conflicts surface via a `conflict` event:

```ts
wsfs.addEventListener("conflict", (event) => {
  console.warn("Conflict detected", event.detail);
});
```

- Pass a `codec` (`encode`/`decode`) to encrypt/compress payloads before hitting storage or the network.

## Server toolkit (`@mfukala/wsfs/server`)

Hook the protocol into any Node.js framework. `createWsfsApi` exposes the core sync/read/write/delete/list methods and works with any `PersistenceAdapter`. An in-memory adapter ships with the package for testing.

### Express server example

```ts
import express from "express";
import { createWsfsApi, MemoryPersistence } from "@mfukala/wsfs/server";

const persistence = new MemoryPersistence(); // bring your own adapter in production
const api = createWsfsApi(persistence);
const app = express();
app.use(express.json({ limit: "5mb" }));

app.post("/sync", async (req, res) => {
  try {
    res.status(200).json(await api.sync(req.body));
  } catch (err: any) {
    res.status(err?.status ?? 500).json({ error: err?.message ?? "sync failed" });
  }
});

app.get("/file", async (req, res) => {
  const file = await api.getFile(String(req.query.path ?? ""));
  if (!file) return res.status(404).end();
  res.json(file);
});

app.get("/file/info", async (req, res) => {
  const info = await api.getFileInfo(String(req.query.path ?? ""));
  if (!info) return res.status(404).end();
  res.json(info);
});

app.get("/list", async (req, res) => {
  res.json(await api.list(String(req.query.prefix ?? "/")));
});

app.put("/file", async (req, res) => {
  try {
    const result = await api.putFile({
      path: req.body.path,
      content: req.body.content,
      contentBase64: req.body.contentBase64,
      encoding: req.body.encoding,
      ifMatch: req.headers["if-match"] as string | undefined,
      updatedBy: req.body.updatedBy,
    });
    res.status(200).json(result);
  } catch (err: any) {
    res.status(err?.status ?? 500).json({ error: err?.message ?? "put failed" });
  }
});

app.delete("/file", async (req, res) => {
  try {
    await api.deleteFile({
      path: String(req.query.path ?? ""),
      ifMatch: req.headers["if-match"] as string | undefined,
    });
    res.status(204).end();
  } catch (err: any) {
    res.status(err?.status ?? 500).json({ error: err?.message ?? "delete failed" });
  }
});

app.listen(8787);
```

### Next.js API Route example

```ts
// pages/api/wsfs/sync.ts (or app/api/wsfs/sync/route.ts)
import { createWsfsApi, MemoryPersistence } from "@mfukala/wsfs/server";

const api = createWsfsApi(new MemoryPersistence()); // swap in your adapter

export default async function handler(req, res) {
  try {
    const result = await api.sync(req.body);
    res.status(200).json(result);
  } catch (err: any) {
    res.status(err?.status ?? 500).json({ error: err?.message ?? "sync failed" });
  }
}
```

Reuse the same pattern for `/file`, `/file/info`, and `/list`, passing the request payloads into `api.putFile`, `api.getFile`, `api.getFileInfo`, and `api.list` while preserving the `If-Match` header for writes/deletes.

### Server endpoints the client expects

- `POST /sync` with `{ prefix, writes, deletes, known, watermark }`
- `GET /file?path=/foo.txt` → `{ etag, encoding, updatedBy?, content|contentBase64 }`
- `GET /file/info?path=/foo.txt` → `{ etag, encoding, updatedBy? }`
- `GET /list?prefix=/dir/` → `[{ path, etag, encoding }]`
- `PUT /file` + `If-Match: <etag|*>` → `{ etag }`
- `DELETE /file?path=...` + `If-Match: <etag|*>`

The bundled `MemoryPersistence` enforces `If-Match` (use `"*"` to create new files), tracks `updatedBy`, and exposes incremental sync via watermarks.

### SQL persistence (driver-agnostic)

`SqlPersistence` stays runtime-agnostic: you supply a tiny executor (`get/all/run` and optional `transaction`) plus partition binders (e.g., `user_id`, `vault_id`). Bring your own driver (SQLite/Postgres/MySQL, etc.) and wire it up:

```ts
import Database from "better-sqlite3";
import { SqlPersistence, createWsfsApi } from "@mfukala/wsfs/server";

const db = new Database(":memory:");
// create `files` + `file_changes` tables with your partition columns

const persistence = new SqlPersistence({
  table: "files",
  changesTable: "file_changes",
  executor: {
    get: (sql, params) => db.prepare(sql).get(params),
    all: (sql, params) => db.prepare(sql).all(params),
    run: (sql, params) => {
      const { changes } = db.prepare(sql).run(params);
      return { rowsAffected: changes };
    },
    transaction: async (fn) => {
      db.exec("BEGIN");
      try {
        const result = await fn();
        db.exec("COMMIT");
        return result;
      } catch (err) {
        db.exec("ROLLBACK");
        throw err;
      }
    },
  },
  partition: { columns: ["user_id", "vault_id"], toParams: (p) => [p.userId, p.vaultId] },
  partitionValue: { userId: "demo", vaultId: "vault-1" },
});

const api = createWsfsApi(persistence);
```

For multi-tenant setups, call `persistence.withPartition({ userId, vaultId })` per request and pass that instance into `createWsfsApi`. Watermarks come from `file_changes.updated_at`, enabling incremental `listChanges`.

Transactions are optional; omit them with drivers that require synchronous callbacks (e.g., better-sqlite3) and rely on single-statement atomicity, or supply an async-friendly transaction wrapper when your driver supports it.
