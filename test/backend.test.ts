import { Buffer } from "node:buffer";
import { expect } from "chai";
import { createWsfsApi } from "../src/backend/backendApi.js";
import type {
  FileRecord,
  ListChangesPage,
  ListPage,
  PersistenceAdapter,
} from "../src/backend/persistence.js";

class FakePersistence implements PersistenceAdapter {
  readonly records: Map<string, FileRecord> = new Map();
  private readonly pages: ListPage[];
  private readonly changes: ListChangesPage[];
  readCalls = 0;
  readManyCalls = 0;
  listCalls = 0;
  listChangesCalls = 0;

  constructor(
    records: FileRecord[],
    pages: ListPage[],
    changes: ListChangesPage[] = [],
  ) {
    records.forEach((record) => this.records.set(record.path, record));
    this.pages = [...pages];
    this.changes = [...changes];
  }

  async read(path: string): Promise<FileRecord | null> {
    this.readCalls += 1;
    return this.records.get(path) ?? null;
  }

  async readMany(paths: string[]): Promise<Array<FileRecord | null>> {
    this.readManyCalls += 1;
    return Promise.all(paths.map((path) => this.records.get(path) ?? null));
  }

  async write(): Promise<FileRecord> {
    throw new Error("write not implemented in FakePersistence");
  }

  async delete(): Promise<void> {
    throw new Error("delete not implemented in FakePersistence");
  }

  async list(_prefix: string, _limit?: number, _cursor?: string): Promise<ListPage> {
    this.listCalls += 1;
    return this.pages.shift() ?? { items: [], cursor: null };
  }

  async listChanges(): Promise<ListChangesPage> {
    this.listChangesCalls += 1;
    return this.changes.shift() ?? { items: [], cursor: null, watermark: null, reset: false };
  }
}

function record(path: string, etag: string, content: string): FileRecord {
  return {
    path,
    etag,
    encoding: "utf8",
    content: Buffer.from(content, "utf8"),
  };
}

describe("server api pagination and bulk reads", () => {
  it("uses readMany when available to fetch remote updates", async () => {
    const remote = record("/a.txt", "etag-2", "new");
    const persistence = new FakePersistence([remote], [
      { items: [{ path: remote.path, etag: remote.etag, encoding: remote.encoding }], cursor: null },
    ]);
    const api = createWsfsApi(persistence);
    const result = await api.sync({
      prefix: "/",
      known: [{ path: remote.path, etag: "old-etag" }],
    });
    expect(result.remoteUpdates).to.have.lengthOf(1);
    expect(result.remoteUpdates[0]?.path).to.equal("/a.txt");
    expect(persistence.readManyCalls).to.equal(1);
    expect(persistence.readCalls).to.equal(0);
  });

  it("aggregates paged list responses before fetching remote updates", async () => {
    const first = record("/first.txt", "etag-1", "one");
    const second = record("/second.txt", "etag-2", "two");
    const persistence = new FakePersistence([first, second], [
      {
        items: [{ path: first.path, etag: first.etag, encoding: first.encoding }],
        cursor: "next",
      },
      {
        items: [{ path: second.path, etag: second.etag, encoding: second.encoding }],
        cursor: null,
      },
    ]);
    const api = createWsfsApi(persistence);
    const result = await api.sync({
      prefix: "/",
      known: [],
    });
    expect(result.remoteUpdates.map((item) => item.path)).to.deep.equal([
      "/first.txt",
      "/second.txt",
    ]);
    expect(persistence.listCalls).to.equal(2);
    expect(persistence.readManyCalls).to.equal(1);
  });

  it("returns watermark without walking full tree when unchanged", async () => {
    const persistence = new FakePersistence([], [], [
      { items: [], cursor: null, watermark: "7", reset: false },
    ]);
    const api = createWsfsApi(persistence);
    const result = await api.sync({
      prefix: "/",
      known: [],
      watermark: "7",
    });
    expect(result.remoteUpdates).to.have.lengthOf(0);
    expect(result.remoteMissing).to.have.lengthOf(0);
    expect(result.watermark).to.equal("7");
    expect(persistence.listChangesCalls).to.equal(1);
    expect(persistence.listCalls).to.equal(0);
  });
});
