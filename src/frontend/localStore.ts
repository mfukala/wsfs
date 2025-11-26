/** Canonical record persisted locally. `content` is optional so we can store
 * tombstones/metadata for deletes without downloading payloads.
 */
export interface LocalEntry {
  /** normalized absolute path */
  path: string;
  /** file payload (undefined for tombstones) */
  content?: string | Uint8Array;
  /** payload encoding, defaults to "utf8" when content is string */
  encoding?: "utf8" | "base64";
  /** last known backend version */
  etag?: string;
  /** optional author metadata */
  updatedBy?: string;
  /** true when awaiting push */
  dirty?: boolean;
  /** true when representing a delete */
  deleted?: boolean;
}

/** Minimal API the sync engine needs from the local cache. */
export interface LocalStore {
  /** fetch a single entry */
  get(path: string): Promise<LocalEntry | null>;
  /** upsert an entry */
  put(entry: LocalEntry): Promise<void>;
  /** drop an entry entirely */
  remove(path: string): Promise<void>;
  /** enumerate by prefix */
  list(prefix: string): Promise<LocalEntry[]>;
  /** pending changes to push */
  listDirty(): Promise<LocalEntry[]>;
  /** retrieve the last known watermark for a prefix */
  getWatermark(prefix: string): Promise<string | null>;
  /** persist the latest watermark for a prefix */
  setWatermark(prefix: string, watermark: string | null): Promise<void>;
}

export function createLocalStore(namespace: string): LocalStore {
  // In Node/tests we lack IndexedDB, so fall back to a process-local map.
  if (typeof indexedDB === "undefined") {
    return new MemoryStore();
  }
  return new IndexedDbStore(namespace);
}

// In-memory adapter for environments without IndexedDB (e.g., SSR/tests).
class MemoryStore implements LocalStore {
  private readonly records = new Map<string, LocalEntry>();
  private readonly watermarks = new Map<string, string>();

  async get(path: string): Promise<LocalEntry | null> {
    return this.records.get(path) ?? null;
  }

  async put(entry: LocalEntry): Promise<void> {
    this.records.set(entry.path, { ...entry });
  }

  async remove(path: string): Promise<void> {
    this.records.delete(path);
  }

  async list(prefix: string): Promise<LocalEntry[]> {
    return [...this.records.values()].filter((entry) =>
      entry.path.startsWith(prefix)
    );
  }

  async listDirty(): Promise<LocalEntry[]> {
    return [...this.records.values()].filter((entry) => !!entry.dirty);
  }

  async getWatermark(prefix: string): Promise<string | null> {
    return this.watermarks.get(prefix) ?? null;
  }

  async setWatermark(prefix: string, watermark: string | null): Promise<void> {
    if (watermark === null) {
      this.watermarks.delete(prefix);
      return;
    }
    this.watermarks.set(prefix, watermark);
  }
}

// Browser-persisted store backed by IndexedDB. Keeps the same API surface as
// the in-memory fallback above.
class IndexedDbStore implements LocalStore {
  private static readonly storeName = "files";
  private static readonly metaStoreName = "meta";

  private readonly dbName: string;
  private readonly dbPromise: Promise<IDBDatabase>;

  constructor(namespace: string) {
    this.dbName = `wsfs:${namespace}`;
    this.dbPromise = this.openDb();
  }

  async get(path: string): Promise<LocalEntry | null> {
    return this.withStore("readonly", (store) => this.request(store.get(path)));
  }

  async put(entry: LocalEntry): Promise<void> {
    await this.withStore("readwrite", (store) => this.request(store.put(entry)));
  }

  async remove(path: string): Promise<void> {
    await this.withStore("readwrite", (store) => this.request(store.delete(path)));
  }

  async list(prefix: string): Promise<LocalEntry[]> {
    const entries = await this.withStore("readonly", (store) =>
      this.request<LocalEntry[] | undefined>(store.getAll()),
    );
    return (entries || []).filter((entry) => entry.path.startsWith(prefix));
  }

  async listDirty(): Promise<LocalEntry[]> {
    const entries = await this.withStore("readonly", (store) =>
      this.request<LocalEntry[] | undefined>(store.getAll()),
    );
    return (entries || []).filter((entry) => !!entry.dirty);
  }

  async getWatermark(prefix: string): Promise<string | null> {
    const record = await this.withMetaStore("readonly", (store) =>
      this.request<{ key: string; value: string } | undefined>(store.get(prefix)),
    );
    return record?.value ?? null;
  }

  async setWatermark(prefix: string, watermark: string | null): Promise<void> {
    if (watermark === null) {
      await this.withMetaStore("readwrite", (store) =>
        this.request(store.delete(prefix)),
      );
      return;
    }
    await this.withMetaStore("readwrite", (store) =>
      this.request(store.put({ key: prefix, value: watermark })),
    );
  }

  private async withStore<T>(
    mode: IDBTransactionMode,
    runner: (store: IDBObjectStore) => T | Promise<T>,
  ): Promise<T> {
    const db = await this.dbPromise;
    return new Promise<T>((resolve, reject) => {
      const tx = db.transaction(IndexedDbStore.storeName, mode);
      const store = tx.objectStore(IndexedDbStore.storeName);
      let result: T;
      Promise.resolve()
        .then(() => runner(store))
        .then((value) => {
          result = value as T;
        })
        .catch((err) => {
          reject(err);
        });
      tx.oncomplete = () => resolve(result!);
      tx.onerror = () =>
        reject(tx.error || new Error("IndexedDB transaction failed"));
      tx.onabort = () =>
        reject(tx.error || new Error("IndexedDB transaction aborted"));
    });
  }

  private async withMetaStore<T>(
    mode: IDBTransactionMode,
    runner: (store: IDBObjectStore) => T | Promise<T>,
  ): Promise<T> {
    const db = await this.dbPromise;
    return new Promise<T>((resolve, reject) => {
      const tx = db.transaction(IndexedDbStore.metaStoreName, mode);
      const store = tx.objectStore(IndexedDbStore.metaStoreName);
      let result: T;
      Promise.resolve()
        .then(() => runner(store))
        .then((value) => {
          result = value as T;
        })
        .catch((err) => {
          reject(err);
        });
      tx.oncomplete = () => resolve(result!);
      tx.onerror = () =>
        reject(tx.error || new Error("IndexedDB transaction failed"));
      tx.onabort = () =>
        reject(tx.error || new Error("IndexedDB transaction aborted"));
    });
  }

  private request<T = unknown>(request: IDBRequest): Promise<T> {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () =>
        reject(request.error || new Error("IndexedDB request failed"));
    });
  }

  private openDb(): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 2);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(IndexedDbStore.storeName)) {
          const store = db.createObjectStore(IndexedDbStore.storeName, {
            keyPath: "path",
          });
          store.createIndex("dirty", "dirty", { unique: false });
        }
        if (!db.objectStoreNames.contains(IndexedDbStore.metaStoreName)) {
          db.createObjectStore(IndexedDbStore.metaStoreName, { keyPath: "key" });
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () =>
        reject(request.error || new Error("Failed to open IndexedDB"));
    });
  }
}
