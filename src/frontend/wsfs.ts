import { createLocalStore, type LocalEntry, type LocalStore } from "./localStore.js";
import { identityCodec, type Codec, type CodecPayload } from "./codec.js";

/** Client initialization options. */
export interface WsfsOptions {
  /** IndexedDB namespace for isolation between apps */
  namespace: string;
  /** Base URL of the REST backend */
  backendUrl: string;
  /** Optional codec hook for encrypting/compressing payloads. */
  codec?: Codec;
  /**
   * Optional hook to attach auth headers/proof payloads to outbound requests.
   * Called for sync/read/list operations before the fetch is dispatched.
   */
  attachAuth?: AttachAuth;
}

type SyncWritePayload = {
  path: string;
  ifMatch?: string;
  content?: string;
  contentBase64?: string;
  encoding?: "utf8" | "base64";
  updatedBy?: string;
  [key: string]: unknown;
};

type SyncDeletePayload = {
  path: string;
  ifMatch?: string;
  [key: string]: unknown;
};

type SyncRequestPayload = {
  prefix: string;
  writes?: SyncWritePayload[];
  deletes?: SyncDeletePayload[];
  known?: Array<{
    path: string;
    etag: string | undefined;
  }>;
  watermark?: string;
  [key: string]: unknown;
};

type AttachAuthKind =
  | "sync"
  | "getFile"
  | "getFileInfo"
  | "putFile"
  | "deleteFile"
  | "list";

type AttachAuthPayloadMap = {
  sync: { headers: Record<string, string>; body: SyncRequestPayload };
  getFile: { headers: Record<string, string>; path: string };
  getFileInfo: { headers: Record<string, string>; path: string };
  putFile: { headers: Record<string, string>; body: SyncWritePayload };
  deleteFile: { headers: Record<string, string>; body: SyncDeletePayload };
  list: { headers: Record<string, string>; prefix: string };
};

export type AttachAuth = <Kind extends AttachAuthKind>(
  kind: Kind,
  payload: AttachAuthPayloadMap[Kind],
) => void | Partial<AttachAuthPayloadMap[Kind]> | Promise<void | Partial<AttachAuthPayloadMap[Kind]>>;

/** Result entry from listing a directory prefix. */
export interface ListEntry {
  /** Absolute, normalized path ("/foo/bar.txt") */
  path: string;
  /** Opaque version stamp from the backend */
  etag: string | undefined;
  /** Content encoding hint ("utf8" | "base64") */
  encoding?: "utf8" | "base64";
  /** Optional author metadata */
  updatedBy?: string;
}

/** Payload emitted on optimistic concurrency conflicts. */
export interface ConflictEventDetail {
  path: string;
  /** Version of the local copy we tried to push */
  localEtag?: string;
  /** Version currently on the backend */
  remoteEtag?: string;
  /** Optional attribution for the remote author */
  updatedBy?: string;
}

/** Operations available inside a read task. */
export interface ReadTaskClient {
  read(path: string): Promise<string | Uint8Array>;
  list(prefix?: string): Promise<ListEntry[]>;
  info(
    path: string,
  ): Promise<{ etag: string | undefined; encoding: "utf8" | "base64"; updatedBy?: string }>;
}

/** Operations available inside a write task. */
export interface WriteTaskClient extends ReadTaskClient {
  write(path: string, content: string | Uint8Array): Promise<void>;
  delete(path: string): Promise<void>;
}

type TaskGuard = { active: boolean; type: "read" | "write" };

type EncodedContent =
  | { content: string; encoding: "utf8" }
  | { contentBase64: string; encoding: "base64" };

type StorePayload = { content: string | Uint8Array; encoding: "utf8" | "base64" };

type RemoteRecord = {
  path: string;
  etag: string;
  encoding: "utf8" | "base64";
  updatedBy?: string;
  content?: string;
  contentBase64?: string;
};

type WriteResult = {
  path: string;
  status: number;
  etag?: string;
  remoteEtag?: string;
  updatedBy?: string;
  error?: string;
};

type DeleteResult = {
  path: string;
  status: number;
  remoteEtag?: string;
  updatedBy?: string;
  error?: string;
};

/**
 * Web Sync File System client.
 */
export class Wsfs extends EventTarget {
  private readonly namespace: string;
  private readonly backendUrl: string;
  private readonly store: LocalStore;
  private readonly codec: Codec;
  private readonly attachAuth?: AttachAuth;
  private readonly textEncoder: TextEncoder;
  private readonly textDecoder: TextDecoder;
  private readonly lock: ReadWriteLock;

  private constructor(options: WsfsOptions, store: LocalStore) {
    super();
    this.textEncoder = new TextEncoder();
    this.textDecoder = new TextDecoder();
    this.lock = new ReadWriteLock();
    this.namespace = options.namespace;
    this.backendUrl = options.backendUrl;
    this.codec = options.codec ?? identityCodec;
    this.attachAuth = options.attachAuth;
    this.store = store;
  }

  static async init(options: WsfsOptions): Promise<Wsfs> {
    const store = createLocalStore(options.namespace);
    /** Touch the store to ensure it initializes before returning. */
    await store.list("/");
    return new Wsfs(options, store);
  }

  private async read(path: string, guard: TaskGuard): Promise<string | Uint8Array> {
    this.assertTaskGuard(guard, false);
    const normalized = this.normalizePath(path);
    const cached = await this.store.get(normalized);
    if (cached && !cached.deleted) {
      /** Cache hit: return local content even when offline. Empty files are
       * stored as "", tombstones have `content` undefined.
       */
      return await this.decodeStoredContent(cached);
    }
    /** Cache miss or tombstone: fetch fresh copy from backend then persist. */
    const remote = await this.fetchFile(normalized);
    if (!remote) {
      throw new Error(`File not found: ${normalized}`);
    }
    await this.store.put({
      path: normalized,
      content: remote.content,
      encoding: remote.encoding,
      etag: remote.etag,
      updatedBy: remote.updatedBy,
      dirty: false,
      deleted: false,
    });
    return await this.decodeStoredContent({
      path: normalized,
      content: remote.content,
      encoding: remote.encoding,
    });
  }

  private async write(
    path: string,
    content: string | Uint8Array,
    guard: TaskGuard,
  ): Promise<void> {
    this.assertTaskGuard(guard, true);
    const normalized = this.normalizePath(path);
    const existing = await this.store.get(normalized);
    /** Queue the change locally; sync will push it with optimistic etag check. */
    const encoded = await this.encodeForStore(normalized, content);
    await this.store.put({
      path: normalized,
      content: encoded.content,
      encoding: encoded.encoding,
      etag: existing?.etag,
      updatedBy: existing?.updatedBy,
      dirty: true,
      deleted: false,
    });
  }

  private async delete(path: string, guard: TaskGuard): Promise<void> {
    this.assertTaskGuard(guard, true);
    const normalized = this.normalizePath(path);
    const existing = await this.store.get(normalized);
    /** Persist tombstone so sync can send DELETE later. */
    await this.store.put({
      path: normalized,
      etag: existing?.etag,
      updatedBy: existing?.updatedBy,
      dirty: true,
      deleted: true,
    });
  }

  private async list(prefix = "/", guard: TaskGuard): Promise<ListEntry[]> {
    this.assertTaskGuard(guard, false);
    const normalized = this.normalizePath(prefix, true);
    const entries = await this.store.list(normalized);
    return entries
      .filter((entry) => !entry.deleted)
      .map((entry) => ({
        path: entry.path,
        etag: entry.etag,
        encoding: entry.encoding,
        updatedBy: entry.updatedBy,
      }));
  }

  private async info(
    path: string,
    guard: TaskGuard,
  ): Promise<{ etag: string | undefined; encoding: "utf8" | "base64"; updatedBy?: string }> {
    this.assertTaskGuard(guard, false);
    const normalized = this.normalizePath(path);
    const local = await this.store.get(normalized);
    if (local && !local.deleted && local.encoding) {
      return { etag: local.etag, encoding: local.encoding, updatedBy: local.updatedBy };
    }
    const headers: Record<string, string> = {};
    await this.applyAttachAuth("getFileInfo", { headers, path: normalized });
    const response = await this.backendFetch(
      `/file/info?path=${encodeURIComponent(normalized)}`,
      { headers },
    );
    if (response.status === 404) {
      throw new Error(`File not found: ${normalized}`);
    }
    if (!response.ok) {
      throw new Error(`Failed to fetch file info ${normalized} (${response.status})`);
    }
    const payload = (await response.json()) as {
      etag: string;
      encoding: "utf8" | "base64";
      updatedBy?: string;
    };
    await this.store.put({
      path: normalized,
      etag: payload.etag,
      encoding: payload.encoding,
      updatedBy: payload.updatedBy,
      dirty: false,
      deleted: false,
    });
    return payload;
  }

  async sync(prefix = "/"): Promise<void> {
    const normalized = this.normalizePath(prefix, true);
    const dirtyEntries = await this.store.listDirty();
    const writes = dirtyEntries.filter((entry) => !entry.deleted);
    const deletes = dirtyEntries.filter((entry) => !!entry.deleted);
    const knownClean = (await this.store.list(normalized)).filter(
      (entry) => !entry.deleted && !entry.dirty,
    );
    const watermark = await this.store.getWatermark(normalized);
    const request: SyncRequestPayload = {
      prefix: normalized,
      writes: writes.map((entry) => ({
        path: entry.path,
        ...this.encodeContent(entry.content),
        ifMatch: entry.etag ?? "*",
      })),
      deletes: deletes.map((entry) => ({
        path: entry.path,
        ifMatch: entry.etag ?? "*",
      })),
      known: knownClean.map((entry) => ({
        path: entry.path,
        etag: entry.etag,
      })),
      watermark: watermark ?? undefined,
    };
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    await this.applyAttachAuth("sync", { headers, body: request });
    headers["Content-Type"] ??= "application/json";
    const response = await this.backendFetch("/sync", {
      method: "POST",
      headers,
      body: JSON.stringify(request),
    });
    if (!response.ok) {
      throw new Error(`Failed to sync (${response.status})`);
    }
    const payload = (await response.json()) as {
      writeResults: WriteResult[];
      deleteResults: DeleteResult[];
      remoteUpdates: RemoteRecord[];
      remoteMissing: string[];
      watermark: string | null;
    };
    await this.applyWriteResults(payload.writeResults, writes);
    await this.applyDeleteResults(payload.deleteResults, deletes);
    await this.applyRemoteUpdates(payload.remoteUpdates);
    await this.applyRemoteMissing(payload.remoteMissing);
    await this.store.setWatermark(normalized, payload.watermark ?? null);
  }

  private async fetchFile(
    path: string,
  ): Promise<{ etag: string; updatedBy?: string; content: string | Uint8Array; encoding: "utf8" | "base64" } | null> {
    const headers: Record<string, string> = {};
    await this.applyAttachAuth("getFile", { headers, path });
    const response = await this.backendFetch(`/file?path=${encodeURIComponent(path)}`, { headers });
    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new Error(`Failed to fetch file ${path} (${response.status})`);
    }
    const payload = (await response.json()) as RemoteRecord;
    const decoded = this.decodeRemoteContent(payload);
    return {
      etag: payload.etag,
      updatedBy: payload.updatedBy,
      content: decoded.content,
      encoding: decoded.encoding,
    };
  }

  private async applyWriteResults(
    results: WriteResult[],
    entries: LocalEntry[],
  ): Promise<void> {
    const entryByPath = new Map(entries.map((entry) => [entry.path, entry]));
    for (const result of results || []) {
      const original = entryByPath.get(result.path);
      if (!original) {
        continue;
      }
      if (result.status === 200 && result.etag) {
        await this.store.put({
          ...original,
          etag: result.etag,
          dirty: false,
          deleted: false,
        });
        continue;
      }
      if (result.status === 412) {
        this.emitConflict({
          path: original.path,
          localEtag: original.etag,
          remoteEtag: result.remoteEtag,
          updatedBy: result.updatedBy,
        });
        continue;
      }
      if (result.status === 428) {
        throw new Error(`Missing If-Match for ${original.path}`);
      }
      throw new Error(
        `Failed to push file ${original.path}: ${result.status} ${result.error || ""}`.trim(),
      );
    }
  }

  private async applyDeleteResults(
    results: DeleteResult[],
    entries: LocalEntry[],
  ): Promise<void> {
    const entryByPath = new Map(entries.map((entry) => [entry.path, entry]));
    for (const result of results || []) {
      const original = entryByPath.get(result.path);
      if (!original) {
        continue;
      }
      if (result.status === 204 || result.status === 200) {
        await this.store.remove(original.path);
        continue;
      }
      if (result.status === 412) {
        this.emitConflict({
          path: original.path,
          localEtag: original.etag,
          remoteEtag: result.remoteEtag,
          updatedBy: result.updatedBy,
        });
        continue;
      }
      if (result.status === 428) {
        throw new Error(`Missing If-Match for ${original.path}`);
      }
      throw new Error(
        `Failed to delete remote file ${original.path}: ${result.status} ${result.error || ""}`.trim(),
      );
    }
  }

  private async applyRemoteUpdates(updates: RemoteRecord[]): Promise<void> {
    for (const update of updates || []) {
      const decoded = this.decodeRemoteContent(update);
      await this.store.put({
        path: update.path,
        content: decoded.content,
        encoding: decoded.encoding,
        etag: update.etag,
        updatedBy: update.updatedBy,
        dirty: false,
        deleted: false,
      });
    }
  }

  private async applyRemoteMissing(paths: string[]): Promise<void> {
    for (const path of paths || []) {
      const local = await this.store.get(path);
      if (local && !local.dirty) {
        /** Remote no longer has this file; mirror the deletion locally. */
        await this.store.remove(path);
      }
    }
  }

  private async decodeStoredContent(entry: LocalEntry): Promise<string | Uint8Array> {
    const encoding =
      entry.encoding ?? (typeof entry.content === "string" ? "utf8" : "base64");
    const rawContent = this.materializeContent(entry, encoding);
    const decoded = await this.codec.decode({
      path: entry.path,
      encoding,
      content: rawContent,
    } as CodecPayload);
    if (decoded.encoding === "base64") {
      return new Uint8Array(decoded.content);
    }
    return this.textDecoder.decode(decoded.content);
  }

  private encodeContent(content: LocalEntry["content"]): EncodedContent {
    if (content === undefined) {
      return { content: "", encoding: "utf8" };
    }
    if (typeof content === "string") {
      return { content, encoding: "utf8" };
    }
    return {
      contentBase64: this.toBase64(content),
      encoding: "base64",
    };
  }

  private async encodeForStore(path: string, content: string | Uint8Array): Promise<StorePayload> {
    const encoding = typeof content === "string" ? "utf8" : "base64";
    const raw =
      typeof content === "string"
        ? this.textEncoder.encode(content)
        : this.asUint8Array(content);
    const encoded = await this.codec.encode({
      path,
      content: raw,
      encoding,
    });
    if (encoded.encoding === "base64") {
      return {
        content: new Uint8Array(encoded.content),
        encoding: "base64",
      };
    }
    return {
      content: this.textDecoder.decode(encoded.content),
      encoding: "utf8",
    };
  }

  private materializeContent(
    entry: Pick<LocalEntry, "content">,
    encoding: "utf8" | "base64",
  ): Uint8Array {
    if (entry.content === undefined) {
      return new Uint8Array(0);
    }
    if (encoding === "base64") {
      if (entry.content instanceof Uint8Array) {
        return entry.content;
      }
      return this.fromBase64(entry.content);
    }
    if (typeof entry.content === "string") {
      return this.textEncoder.encode(entry.content);
    }
    return this.asUint8Array(entry.content);
  }

  private asUint8Array(input: ArrayBufferView | ArrayBuffer): Uint8Array {
    if (input instanceof Uint8Array) {
      return input;
    }
    if (ArrayBuffer.isView(input)) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    return new Uint8Array(input);
  }

  private decodeRemoteContent(
    payload: RemoteRecord,
  ): { content: string | Uint8Array; encoding: "utf8" | "base64" } {
    if (payload.encoding === "base64" || payload.contentBase64 !== undefined) {
      return {
        content: this.fromBase64(payload.contentBase64 ?? payload.content ?? ""),
        encoding: "base64",
      };
    }
    return {
      content: payload.content ?? "",
      encoding: "utf8",
    };
  }

  private toBase64(data: Uint8Array): string {
    const maybeBuffer = (globalThis as typeof globalThis & { Buffer?: typeof Buffer }).Buffer;
    if (maybeBuffer) {
      return maybeBuffer.from(data).toString("base64");
    }
    let binary = "";
    data.forEach((byte) => {
      binary += String.fromCharCode(byte);
    });
    return btoa(binary);
  }

  private fromBase64(base64: string): Uint8Array {
    const maybeBuffer = (globalThis as typeof globalThis & { Buffer?: typeof Buffer }).Buffer;
    if (maybeBuffer) {
      return new Uint8Array(maybeBuffer.from(base64, "base64"));
    }
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }

  private async backendFetch(url: string, init?: RequestInit): Promise<Response> {
    const fullUrl = `${this.backendUrl}${url}`;
    return await fetch(fullUrl, init);
  }

  private async applyAttachAuth<Kind extends AttachAuthKind>(
    kind: Kind,
    payload: AttachAuthPayloadMap[Kind],
  ): Promise<void> {
    if (!this.attachAuth) {
      return;
    }
    const result = await this.attachAuth(kind, payload);
    if (!result) {
      return;
    }
    if ("headers" in result && result.headers) {
      payload.headers = { ...payload.headers, ...result.headers };
    }
    if ("body" in result && "body" in payload && result.body !== undefined) {
      (payload as { body: unknown }).body = result.body as (typeof payload)["body"];
    }
  }

  /**
   * Run a read-only task that may use wsfs APIs concurrently with other reads.
   */
  async runReadTask<T>(task: (client: ReadTaskClient) => Promise<T>): Promise<T> {
    const release = await this.lock.acquireRead();
    const guard = this.createTaskGuard("read");
    const client = this.createReadClient(guard);
    try {
      return await task(client);
    } finally {
      this.finishTask(guard);
      release();
    }
  }

  /**
   * Run a write task exclusively; on failure, revert local state to a snapshot
   * taken before the task started.
   */
  async runWriteTask<T>(task: (client: WriteTaskClient) => Promise<T>): Promise<T> {
    const release = await this.lock.acquireWrite();
    const snapshot = await this.snapshotStore();
    const guard = this.createTaskGuard("write");
    const client = this.createWriteClient(guard);
    try {
      return await task(client);
    } catch (err) {
      await this.restoreSnapshot(snapshot);
      throw err;
    } finally {
      this.finishTask(guard);
      release();
    }
  }

  /**
   * Run a write task and then attempt to sync while still holding the write
   * lock. Local changes are reverted if the task itself fails; sync failures do
   * not rollback local edits.
   */
  async runWriteTaskAndSync<T>(
    task: (client: WriteTaskClient) => Promise<T>,
    prefix = "/",
  ): Promise<T> {
    const release = await this.lock.acquireWrite();
    const snapshot = await this.snapshotStore();
    const guard = this.createTaskGuard("write");
    const client = this.createWriteClient(guard);
    let result: T;
    try {
      result = await task(client);
    } catch (err) {
      await this.restoreSnapshot(snapshot);
      this.finishTask(guard);
      release();
      throw err;
    }
    this.finishTask(guard);
    try {
      await this.sync(prefix);
    } finally {
      release();
    }
    return result;
  }

  private normalizePath(path: string, treatAsPrefix = false): string {
    if (!path.startsWith("/")) {
      path = `/${path}`;
    }
    if (!treatAsPrefix && path.endsWith("/") && path !== "/") {
      path = path.replace(/\/+$/, "");
    }
    return path;
  }

  /** Capture a deep copy of the local store for later rollback. */
  private async snapshotStore(): Promise<LocalEntry[]> {
    const entries = await this.store.list("/");
    return entries.map((entry) => ({
      ...entry,
      content:
        entry.content instanceof Uint8Array
          ? new Uint8Array(entry.content)
          : entry.content,
    }));
  }

  private async restoreSnapshot(snapshot: LocalEntry[]): Promise<void> {
    const snapshotMap = new Map(snapshot.map((entry) => [entry.path, entry]));
    const current = await this.store.list("/");
    for (const entry of current) {
      if (!snapshotMap.has(entry.path)) {
        await this.store.remove(entry.path);
      }
    }
    for (const entry of snapshot) {
      await this.store.put({
        ...entry,
        content:
          entry.content instanceof Uint8Array
            ? new Uint8Array(entry.content)
            : entry.content,
      });
    }
  }

  private emitConflict(detail: ConflictEventDetail): void {
    const event = new CustomEvent<ConflictEventDetail>("conflict", {
      detail,
    });
    this.dispatchEvent(event);
  }

  private createTaskGuard(type: TaskGuard["type"]): TaskGuard {
    return { active: true, type };
  }

  private finishTask(guard: TaskGuard): void {
    guard.active = false;
  }

  private assertTaskGuard(guard: TaskGuard | null | undefined, requiresWrite: boolean): void {
    if (!guard?.active) {
      throw new Error("File operations are only allowed inside an active task.");
    }
    if (requiresWrite && guard.type !== "write") {
      throw new Error("This operation is only allowed inside a write task.");
    }
  }

  private createReadClient(guard: TaskGuard): ReadTaskClient {
    return {
      read: (path: string) => this.read(path, guard),
      list: (prefix?: string) => this.list(prefix ?? "/", guard),
      info: (path: string) => this.info(path, guard),
    };
  }

  private createWriteClient(guard: TaskGuard): WriteTaskClient {
    return {
      ...this.createReadClient(guard),
      write: (path: string, content: string | Uint8Array) =>
        this.write(path, content, guard),
      delete: (path: string) => this.delete(path, guard),
    };
  }
}

/** Minimal read/write lock: write tasks are exclusive, reads run together. */
class ReadWriteLock {
  private activeReaders = 0;
  private writerActive = false;
  private pendingReads: Array<() => void> = [];
  private pendingWrites: Array<() => void> = [];

  async acquireRead(): Promise<() => void> {
    if (this.canReadImmediately()) {
      this.activeReaders++;
      return () => this.releaseRead();
    }
    return new Promise((resolve) => {
      this.pendingReads.push(() => {
        this.activeReaders++;
        resolve(() => this.releaseRead());
      });
    });
  }

  async acquireWrite(): Promise<() => void> {
    if (this.canWriteImmediately()) {
      this.writerActive = true;
      return () => this.releaseWrite();
    }
    return new Promise((resolve) => {
      this.pendingWrites.push(() => {
        this.writerActive = true;
        resolve(() => this.releaseWrite());
      });
    });
  }

  private canReadImmediately(): boolean {
    return !this.writerActive && this.pendingWrites.length === 0;
  }

  private canWriteImmediately(): boolean {
    return !this.writerActive && this.activeReaders === 0;
  }

  private releaseRead(): void {
    this.activeReaders = Math.max(0, this.activeReaders - 1);
    if (this.activeReaders === 0) {
      this.drain();
    }
  }

  private releaseWrite(): void {
    this.writerActive = false;
    this.drain();
  }

  private drain(): void {
    if (this.writerActive) {
      return;
    }
    if (this.pendingWrites.length > 0 && this.activeReaders === 0) {
      const nextWrite = this.pendingWrites.shift();
      if (nextWrite) {
        nextWrite();
      }
      return;
    }
    if (this.pendingWrites.length === 0 && this.pendingReads.length > 0) {
      const readers = [...this.pendingReads];
      this.pendingReads = [];
      for (const start of readers) {
        start();
      }
    }
  }
}
