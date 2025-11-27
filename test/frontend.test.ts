import { Buffer } from "node:buffer";
import type { Server } from "node:http";
import Database from "better-sqlite3";
import { expect } from "chai";
import { Wsfs, type ConflictEventDetail } from "../src/frontend/wsfs.js";
import { createWsfsServer } from "./server.js";
import { MemoryPersistence } from "../src/backend/memoryPersistence.js";
import { SqlPersistence } from "../src/backend/sqlPersistence.js";
import type { AuthorizeHook, PartitionSelector } from "../src/backend/backendApi.js";
import type { Codec, CodecPayload } from "../src/frontend/codec.js";
import type { PersistenceAdapter } from "../src/backend/persistence.js";

type Boot = {
  server: Server;
  baseUrl: string;
  persistence: PersistenceAdapter;
  cleanup?: () => void;
};

type Partition = { namespace: string };

function createMemoryPersistence(): { persistence: PersistenceAdapter } {
  return { persistence: new MemoryPersistence() };
}

function createSqlPersistence(): { persistence: PersistenceAdapter; cleanup: () => void } {
  const db = new Database(":memory:");
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE files (
      namespace TEXT NOT NULL,
      path TEXT NOT NULL,
      etag TEXT NOT NULL,
      encoding TEXT NOT NULL,
      content BLOB NOT NULL,
      updated_by TEXT NULL,
      updated_at INTEGER NOT NULL,
      deleted INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(namespace, path)
    );
    CREATE INDEX idx_files_namespace_path ON files(namespace, path);
    CREATE INDEX idx_files_namespace_updated_at ON files(namespace, updated_at);
    CREATE TABLE file_changes (
      namespace TEXT NOT NULL,
      path TEXT NOT NULL,
      etag TEXT NULL,
      encoding TEXT NULL,
      deleted INTEGER NOT NULL,
      updated_by TEXT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX idx_changes_namespace_updated_at ON file_changes(namespace, updated_at);
  `);

  const persistence = new SqlPersistence<Partition>({
    table: "files",
    changesTable: "file_changes",
    executor: {
      async get<T = unknown>(sql: string, params: unknown[]): Promise<T | undefined> {
        return db.prepare(sql).get(params) as T | undefined;
      },
      async all<T = unknown>(sql: string, params: unknown[]): Promise<T[]> {
        return db.prepare(sql).all(params) as T[];
      },
      async run(sql: string, params: unknown[]) {
        const { changes } = db.prepare(sql).run(params);
        return { rowsAffected: changes };
      },
      async transaction<T>(fn: () => Promise<T>): Promise<T> {
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
    partition: { columns: ["namespace"], toParams: (p) => [p.namespace] },
    partitionValue: { namespace: "test" },
  });

  return { persistence, cleanup: () => db.close() };
}

async function startTestServer(
  factory: () => { persistence: PersistenceAdapter; cleanup?: () => void },
  options?: { authorize?: AuthorizeHook; partition?: PartitionSelector },
): Promise<Boot> {
  const { persistence, cleanup } = factory();
  const app = createWsfsServer({
    persistence,
    authorize: options?.authorize,
    partition: options?.partition,
  });
  const server = await new Promise<Server>((resolve) => {
    const handle = app.listen(0, "127.0.0.1", () => resolve(handle));
  });
  const port = (server.address() as { port: number }).port;
  const baseUrl = `http://127.0.0.1:${port}`;
  return { server, baseUrl, persistence, cleanup };
}

async function stopServer(server: Server | null): Promise<void> {
  if (!server) return;
  await new Promise<void>((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

function uniqueNamespace(): string {
  return `wsfs-test-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function createClient(backendUrl: string): Promise<Wsfs> {
  return Wsfs.init({
    namespace: uniqueNamespace(),
    backendUrl,
  });
}

const persistenceFactories: Array<{
  name: string;
  factory: () => { persistence: PersistenceAdapter; cleanup?: () => void };
}> = [
  { name: "memory persistence", factory: createMemoryPersistence },
  { name: "SQL persistence", factory: createSqlPersistence },
];

describe("auth + partition hooks", () => {
  it("blocks sync when authorize rejects the request", async () => {
    const boot = await startTestServer(createMemoryPersistence, {
      authorize: (kind) => {
        if (kind === "sync") {
          const err = new Error("bad proof");
          (err as Error & { status?: number }).status = 403;
          throw err;
        }
      },
    });
    try {
      const response = await fetch(`${boot.baseUrl}/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prefix: "/", known: [] }),
      });
      expect(response.status).to.equal(403);
      const payload = (await response.json()) as { error: string };
      expect(payload.error).to.match(/bad proof/);
    } finally {
      await stopServer(boot.server);
      boot.cleanup?.();
    }
  });

  it("passes attachAuth headers and payload fields through to authorize", async () => {
    const seen: Array<{ headers?: unknown; body?: unknown }> = [];
    const boot = await startTestServer(createMemoryPersistence, {
      authorize: (kind, payload) => {
        if (kind === "sync") {
          const syncPayload = payload as { headers?: unknown; body?: unknown };
          seen.push({ headers: syncPayload.headers, body: syncPayload.body });
        }
      },
    });
    const wsfs = await Wsfs.init({
      namespace: uniqueNamespace(),
      backendUrl: boot.baseUrl,
      attachAuth: (kind, payload) => {
        payload.headers["x-proof"] = "header-token";
        if (kind === "sync") {
          const syncPayload = payload as { body: { writes?: Array<Record<string, unknown>> } };
          syncPayload.body.writes?.forEach((write) => {
            (write as Record<string, unknown>).proof = `proof-${write.path as string}`;
          });
        }
      },
    });
    try {
      await wsfs.runWriteTask(async (client) => {
        await client.write("/secure/a.txt", "secret");
      });
      await wsfs.sync();
      expect(seen).to.have.length(1);
      const record = seen[0];
      expect((record?.headers as Record<string, unknown>)?.["x-proof"]).to.equal("header-token");
      const writes = (record?.body as { writes?: Array<Record<string, unknown>> } | undefined)?.writes;
      expect(writes?.[0]?.proof).to.equal("proof-/secure/a.txt");
    } finally {
      await stopServer(boot.server);
      boot.cleanup?.();
    }
  });

  it("selects partitions via the partition hook", async () => {
    const boot = await startTestServer(createSqlPersistence, {
      partition: (ctx) => {
        const header = ctx.headers?.["x-namespace"];
        const namespace = Array.isArray(header) ? header[0] : header;
        return namespace ? { namespace } : undefined;
      },
    });
    const wsfs = await Wsfs.init({
      namespace: uniqueNamespace(),
      backendUrl: boot.baseUrl,
      attachAuth: (_kind, payload) => {
        payload.headers["x-namespace"] = "tenant-b";
      },
    });
    try {
      await wsfs.runWriteTask(async (client) => {
        await client.write("/partitioned/file.txt", "scoped");
      });
      await wsfs.sync();
      const sql = boot.persistence as SqlPersistence<Partition>;
      const tenantScoped = sql.withPartition({ namespace: "tenant-b" });
      const defaultScoped = sql.withPartition({ namespace: "test" });
      expect(await tenantScoped.read("/partitioned/file.txt")).to.not.equal(null);
      expect(await defaultScoped.read("/partitioned/file.txt")).to.equal(null);
    } finally {
      await stopServer(boot.server);
      boot.cleanup?.();
    }
  });
});

for (const { name, factory } of persistenceFactories) {
  describe(`wsfs client against server with ${name}`, () => {
    let server: Server | null;
    let baseUrl: string;
    let persistence: PersistenceAdapter;
    let cleanup: (() => void) | undefined;

    beforeEach(async () => {
      const boot = await startTestServer(factory);
      server = boot.server;
      baseUrl = boot.baseUrl;
      persistence = boot.persistence;
      cleanup = boot.cleanup;
    });

    afterEach(async () => {
      await stopServer(server);
      server = null;
      if (cleanup) {
        cleanup();
        cleanup = undefined;
      }
    });

  describe("synchronization", () => {
    it("pushes new writes to the backend", async () => {
      const wsfs = await createClient(baseUrl);
      await wsfs.runWriteTask(async (client) => {
        await client.write("/docs/hello.txt", "hello world");
      });
      expect(await persistence.read("/docs/hello.txt")).to.equal(null);
      await wsfs.sync();
      const stored = await persistence.read("/docs/hello.txt");
      expect(stored?.content.toString("utf8")).to.equal("hello world");
      expect(stored?.encoding).to.equal("utf8");
    });

    it("propagates deletes to the backend", async () => {
      const wsfs = await createClient(baseUrl);
      await wsfs.runWriteTask(async (client) => {
        await client.write("/tmp/file.txt", "temp");
      });
      await wsfs.sync();
      await wsfs.runWriteTask(async (client) => {
        await client.delete("/tmp/file.txt");
      });
      await wsfs.sync();
      expect(await persistence.read("/tmp/file.txt")).to.equal(null);
    });

    it("pulls remote updates and refreshes cached content", async () => {
      const original = await persistence.write("/sync/file.txt", "v1", {
        ifMatch: "*",
        encoding: "utf8",
      });
      const wsfs = await createClient(baseUrl);
      await wsfs.runReadTask((client) => client.read("/sync/file.txt"));
      const updated = await persistence.write("/sync/file.txt", "v2-remote", {
        ifMatch: original.etag,
        encoding: "utf8",
      });
      await wsfs.sync();
      const latest = await wsfs.runReadTask((client) => client.read("/sync/file.txt"));
      expect(latest).to.equal("v2-remote");
      const info = await wsfs.runReadTask((client) => client.info("/sync/file.txt"));
      expect(info.etag).to.equal(updated.etag);
    });

    it("drops locally cached files that disappeared remotely", async () => {
      const remote = await persistence.write("/gone/file.txt", "present", {
        ifMatch: "*",
        encoding: "utf8",
      });
      const wsfs = await createClient(baseUrl);
      await wsfs.runReadTask((client) => client.read("/gone/file.txt"));
      await persistence.delete("/gone/file.txt", { ifMatch: remote.etag });
      await wsfs.sync();
      let error: Error | null = null;
      try {
        await wsfs.runReadTask((client) => client.read("/gone/file.txt"));
      } catch (err: unknown) {
        error = err as Error;
      }
      expect(error?.message).to.match(/File not found/);
    });
  });

  describe("offline caching", () => {
    it("serves cached reads while offline", async () => {
      const wsfs = await createClient(baseUrl);
      await wsfs.runWriteTask(async (client) => {
        await client.write("/offline/cache.txt", "cached");
      });
      await stopServer(server);
      server = null;
      const result = await wsfs.runReadTask((client) => client.read("/offline/cache.txt"));
      expect(result).to.equal("cached");
    });

    it("caches info responses for offline use", async () => {
      const remote = await persistence.write("/meta/base64.bin", Buffer.from([1, 2]), {
        ifMatch: "*",
        encoding: "base64",
        updatedBy: "remote-user",
      });
      const wsfs = await createClient(baseUrl);
      const first = await wsfs.runReadTask((client) => client.info("/meta/base64.bin"));
      expect(first).to.deep.equal({
        etag: remote.etag,
        encoding: "base64",
        updatedBy: "remote-user",
      });
      await stopServer(server);
      server = null;
      const cached = await wsfs.runReadTask((client) => client.info("/meta/base64.bin"));
      expect(cached).to.deep.equal({
        etag: remote.etag,
        encoding: "base64",
        updatedBy: "remote-user",
      });
    });

    it("surfaces updatedBy from cached entries in list/info", async () => {
      const remote = await persistence.write("/meta/attributed.txt", "hello", {
        ifMatch: "*",
        encoding: "utf8",
        updatedBy: "author-a",
      });
      const wsfs = await createClient(baseUrl);
      await wsfs.runReadTask((client) => client.read("/meta/attributed.txt"));
      const entries = await wsfs.runReadTask((client) => client.list("/meta"));
      const entry = entries.find((item) => item.path === "/meta/attributed.txt");
      expect(entry?.updatedBy).to.equal("author-a");
      const info = await wsfs.runReadTask((client) => client.info("/meta/attributed.txt"));
      expect(info).to.deep.equal({
        etag: remote.etag,
        encoding: "utf8",
        updatedBy: "author-a",
      });
    });
  });

  describe("conflict handling", () => {
    it("emits conflict events when remote etag mismatches", async () => {
      const seeded = await persistence.write("/notes/item.txt", "v1", {
        ifMatch: "*",
        encoding: "utf8",
      });
      const wsfs = await createClient(baseUrl);
      await wsfs.runReadTask((client) => client.read("/notes/item.txt"));
      const remoteUpdate = await persistence.write("/notes/item.txt", "server-new", {
        ifMatch: seeded.etag,
        encoding: "utf8",
        updatedBy: "remote-user",
      });
      const conflictPromise = new Promise<ConflictEventDetail>((resolve) => {
        wsfs.addEventListener("conflict", (event: Event) =>
          resolve((event as CustomEvent<ConflictEventDetail>).detail),
        );
      });
      await wsfs.runWriteTask(async (client) => {
        await client.write("/notes/item.txt", "client-update");
      });
      await wsfs.sync();
      const detail = await conflictPromise;
      expect(detail.path).to.equal("/notes/item.txt");
      expect(detail.localEtag).to.equal(seeded.etag);
      expect(detail.remoteEtag).to.equal(remoteUpdate.etag);
      expect(detail.updatedBy).to.equal("remote-user");
      const remote = await persistence.read("/notes/item.txt");
      expect(remote?.content.toString("utf8")).to.equal("server-new");
    });

    it("emits conflict during runWriteTaskAndSync without dropping local edits", async () => {
      const seed = await persistence.write("/conflict/update.txt", "remote", {
        ifMatch: "*",
        encoding: "utf8",
      });
      const wsfs = await createClient(baseUrl);
      await wsfs.runReadTask((client) => client.read("/conflict/update.txt"));
      const newer = await persistence.write("/conflict/update.txt", "remote-new", {
        ifMatch: seed.etag,
        encoding: "utf8",
      });
      const conflictPromise = new Promise<ConflictEventDetail>((resolve) => {
        wsfs.addEventListener("conflict", (event: Event) =>
          resolve((event as CustomEvent<ConflictEventDetail>).detail),
        );
      });
      await wsfs.runWriteTaskAndSync(async (client) => {
        await client.write("/conflict/update.txt", "local-change");
      });
      const detail = await conflictPromise;
      expect(detail.path).to.equal("/conflict/update.txt");
      expect(detail.remoteEtag).to.equal(newer.etag);
      const localAfterSync = await wsfs.runReadTask((client) =>
        client.read("/conflict/update.txt"),
      );
      expect(localAfterSync).to.equal("remote-new");
    });
  });

  describe("write tasks and local state", () => {
    it("rolls back a write task when the task throws", async () => {
      const seeded = await persistence.write("/docs/undo.txt", "original", {
        ifMatch: "*",
        encoding: "utf8",
      });
      const wsfs = await createClient(baseUrl);
      await wsfs.runReadTask((client) => client.read("/docs/undo.txt"));
      let error: Error | null = null;
      try {
        await wsfs.runWriteTask(async (client) => {
          await client.write("/docs/undo.txt", "should-be-rolled-back");
          throw new Error("boom");
        });
      } catch (err: unknown) {
        error = err as Error;
      }
      expect(error?.message).to.equal("boom");
      const local = await wsfs.runReadTask((client) => client.read("/docs/undo.txt"));
      expect(local).to.equal("original");
      await wsfs.sync();
      const remote = await persistence.read("/docs/undo.txt");
      expect(remote?.etag).to.equal(seeded.etag);
      expect(remote?.content.toString("utf8")).to.equal("original");
    });

    it("lists entries without showing local tombstones", async () => {
      const wsfs = await createClient(baseUrl);
      await wsfs.runWriteTask(async (client) => {
        await client.write("/list/a.txt", "keep");
        await client.write("/list/b.txt", "remove");
      });
      await wsfs.runWriteTask(async (client) => {
        await client.delete("/list/b.txt");
      });
      const entries = await wsfs.runReadTask((client) => client.list("/list"));
      const names = entries.map((e) => e.path);
      expect(names).to.deep.equal(["/list/a.txt"]);
    });
  });

  describe("encoding and codecs", () => {
    it("roundtrips binary content using base64", async () => {
      const wsfs = await createClient(baseUrl);
      const binary = new Uint8Array([0, 255, 10, 200, 42]);
      await wsfs.runWriteTaskAndSync(async (client) => {
        await client.write("/bin/blob.bin", binary);
      });
      const remote = await persistence.read("/bin/blob.bin");
      expect(remote?.encoding).to.equal("base64");
      expect(Buffer.compare(remote?.content ?? Buffer.alloc(0), Buffer.from(binary))).to.equal(0);
      const roundtrip = await wsfs.runReadTask((client) => client.read("/bin/blob.bin"));
      expect(roundtrip).to.be.instanceOf(Uint8Array);
      expect(Buffer.from(roundtrip as Uint8Array).equals(Buffer.from(binary))).to.equal(true);
    });

    it("invokes codec on cached reads and writes without needing the backend", async () => {
      const encodeCalls: CodecPayload[] = [];
      const decodeCalls: CodecPayload[] = [];
      const codec: Codec = {
        encode: async (payload) => {
          encodeCalls.push(payload);
          return {
            path: payload.path,
            encoding: "base64",
            content: payload.content.map((byte) => byte ^ 0xaa),
          };
        },
        decode: async (payload) => {
          decodeCalls.push(payload);
          return {
            path: payload.path,
            encoding: "utf8",
            content: payload.content.map((byte) => byte ^ 0xaa),
          };
        },
      };
      const wsfs = await Wsfs.init({
        namespace: uniqueNamespace(),
        backendUrl: baseUrl,
        codec,
      });
      await wsfs.runWriteTask(async (client) => {
        await client.write("/codec/local-only.txt", "hi cache");
      });
      const cached = await wsfs.runReadTask((client) =>
        client.read("/codec/local-only.txt"),
      );
      expect(cached).to.equal("hi cache");
      expect(encodeCalls).to.have.lengthOf(1);
      expect(encodeCalls[0]?.path).to.equal("/codec/local-only.txt");
      expect(encodeCalls[0]?.encoding).to.equal("utf8");
      expect(decodeCalls).to.have.lengthOf(1);
      expect(decodeCalls[0]?.encoding).to.equal("base64");
    });

    it("lets callers plug in a codec for custom encryption/compression", async () => {
      const codec: Codec = {
        encode: ({ content, path }) => ({
          path,
          encoding: "base64",
          content: content.map((byte) => byte ^ 0xaa),
        }),
        decode: ({ content, path }) => ({
          path,
          encoding: "utf8",
          content: content.map((byte) => byte ^ 0xaa),
        }),
      };
      const wsfs = await Wsfs.init({
        namespace: uniqueNamespace(),
        backendUrl: baseUrl,
        codec,
      });
      await wsfs.runWriteTaskAndSync(async (client) => {
        await client.write("/codec/secret.txt", "top-secret");
      });
      const stored = await persistence.read("/codec/secret.txt");
      const expectedEncoded = await codec.encode({
        path: "/codec/secret.txt",
        content: Buffer.from("top-secret", "utf8"),
        encoding: "utf8",
      });
      expect(stored?.encoding).to.equal("base64");
      expect(Buffer.from(stored?.content ?? []).equals(Buffer.from(expectedEncoded.content))).to
        .equal(true);
      const cached = await wsfs.runReadTask((client) => client.read("/codec/secret.txt"));
      expect(cached).to.equal("top-secret");
      const remoteEncoded = await codec.encode({
        path: "/codec/secret.txt",
        content: Buffer.from("rotated-remote", "utf8"),
        encoding: "utf8",
      });
      expect(stored?.etag).to.be.a("string");
      await persistence.write("/codec/secret.txt", Buffer.from(remoteEncoded.content), {
        ifMatch: stored?.etag,
        encoding: remoteEncoded.encoding,
      });
      await wsfs.sync();
      const latest = await wsfs.runReadTask((client) => client.read("/codec/secret.txt"));
      expect(latest).to.equal("rotated-remote");
    });
  });
});
}
