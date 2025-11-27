import Database, { Database as BetterSqliteDatabase } from "better-sqlite3";

import { expect } from "chai";
import { SqlPersistence } from "../src/backend/sqlPersistence.js";
import {
  EtagMismatchError,
  MissingPreconditionError,
} from "../src/backend/persistence.js";

type Namespace = string;

function makeDb(): BetterSqliteDatabase {
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
  return db;
}

function makeExecutor(db: BetterSqliteDatabase) {
  return {
    async get<T = any>(sql: string, params: unknown[]): Promise<T | undefined> {
      return db.prepare(sql).get(params) as T | undefined;
    },
    async all<T = any>(sql: string, params: unknown[]): Promise<T[]> {
      return db.prepare(sql).all(params) as T[];
    },
    async run(sql: string, params: unknown[]): Promise<{ rowsAffected: number }> {
      const result = db.prepare(sql).run(params);
      return { rowsAffected: result.changes };
    },
  };
}

function makeAdapter(
  db: BetterSqliteDatabase,
  namespace: Namespace,
  clockStart = 1,
): SqlPersistence<Namespace> {
  let clock = clockStart;
  return new SqlPersistence<Namespace>({
    table: "files",
    changesTable: "file_changes",
    executor: makeExecutor(db),
    partition: {
      columns: ["namespace"],
      toParams: (ns) => [ns],
    },
    partitionValue: namespace,
    now: () => clock++,
  });
}

async function expectError(fn: () => Promise<unknown>, ctor: new (...args: any[]) => Error) {
  try {
    await fn();
    expect.fail("Expected error to be thrown");
  } catch (err) {
    expect(err).to.be.instanceOf(ctor);
  }
}

describe("SqlPersistence (better-sqlite3)", () => {
  it("enforces if-match and round-trips content", async () => {
    const db = makeDb();
    const adapter = makeAdapter(db, "alpha");

    await expectError(
      () => adapter.write("/missing.txt", "fail", { ifMatch: undefined as any }),
      MissingPreconditionError,
    );

    await expectError(
      () => adapter.write("/missing.txt", "fail", { ifMatch: "etag-1" }),
      EtagMismatchError,
    );

    const created = await adapter.write("/file.txt", "hello", {
      ifMatch: "*",
      updatedBy: "user-1",
    });
    expect(created.path).to.equal("/file.txt");
    expect(created.encoding).to.equal("utf8");

    const fetched = await adapter.read("/file.txt");
    expect(fetched?.etag).to.equal(created.etag);
    expect(fetched?.content.toString("utf8")).to.equal("hello");
    expect(fetched?.updatedBy).to.equal("user-1");

    const updated = await adapter.write("/file.txt", Buffer.from("bye"), {
      ifMatch: created.etag,
      encoding: "utf8",
      updatedBy: "user-2",
    });
    expect(updated.etag).to.not.equal(created.etag);
    expect((await adapter.read("/file.txt"))?.content.toString("utf8")).to.equal("bye");

    await expectError(
      () => adapter.write("/file.txt", "stale", { ifMatch: created.etag }),
      EtagMismatchError,
    );
  });

  it("lists by prefix and hides deleted records", async () => {
    const db = makeDb();
    const adapter = makeAdapter(db, "alpha");

    const first = await adapter.write("/foo/a.txt", "one", { ifMatch: "*" });
    const second = await adapter.write("/bar/b.txt", "two", { ifMatch: "*" });
    await adapter.delete("/bar/b.txt", { ifMatch: second.etag });

    const fooList = await adapter.list("/foo");
    expect(fooList).to.deep.equal([
      { path: "/foo/a.txt", etag: first.etag, encoding: "utf8" },
    ]);

    const rootList = await adapter.list("/");
    expect(rootList).to.deep.equal([
      { path: "/foo/a.txt", etag: first.etag, encoding: "utf8" },
    ]);
  });

  it("returns incremental changes with watermark and tombstones", async () => {
    const db = makeDb();
    const adapter = makeAdapter(db, "alpha", 10); // deterministic clock

    const created = await adapter.write("/item.txt", "v1", { ifMatch: "*" });
    const updated = await adapter.write("/item.txt", "v2", { ifMatch: created.etag });
    await adapter.delete("/item.txt", { ifMatch: updated.etag });

    const allChanges = await adapter.listChanges("/");
    expect(allChanges.watermark).to.equal("12");
    expect(allChanges.items.map((c) => c.deleted)).to.deep.equal([false, false, true]);

    const sinceUpdate = await adapter.listChanges("/", "11");
    expect(sinceUpdate.items).to.have.length(1);
    expect(sinceUpdate.items[0]?.deleted).to.equal(true);

    const reset = await adapter.listChanges("/", "9999");
    expect(reset.reset).to.equal(true);
  });

  it("isolates partitions", async () => {
    const db = makeDb();
    const alpha = makeAdapter(db, "alpha");
    const beta = alpha.withPartition("beta");

    const created = await alpha.write("/shared.txt", "hello", { ifMatch: "*" });
    expect(await beta.read("/shared.txt")).to.equal(null);

    const alphaChanges = await alpha.listChanges("/");
    const betaChanges = await beta.listChanges("/");
    expect(alphaChanges.items).to.have.length(1);
    expect(betaChanges.items).to.have.length(0);
  });
});
