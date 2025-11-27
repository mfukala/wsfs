import { Buffer } from "node:buffer";
import {
  EtagMismatchError,
  FileRecord,
  MissingPreconditionError,
  PersistenceAdapter,
  type ListChangeItem,
} from "./persistence.js";

/** Raised on invalid/missing input before hitting persistence. */
export class BadRequestError extends Error {
  status: number;
  constructor(message: string) {
    super(message);
    this.status = 400;
  }
}

/** User-supplied content accepted by write-like endpoints. */
type IncomingContent = {
  /** Raw string or Buffer for utf8 payloads. */
  content?: unknown;
  /** Base64-encoded payload for binary content. */
  contentBase64?: string;
  /** Force decode logic (defaults to utf8 when not set). */
  encoding?: "utf8" | "base64";
  /** Optional attribution for the writer. */
  updatedBy?: string;
  /** Allow extra auth/proof metadata to ride along with writes. */
  [key: string]: unknown;
};

type WritePayload = IncomingContent & {
  path: string;
  ifMatch?: string;
};

type DeletePayload = {
  path: string;
  ifMatch?: string;
  [key: string]: unknown;
};

type RequestHeaders = Record<string, string | string[] | undefined>;

type AuthorizePayload = {
  sync: { body: SyncRequestBody; headers?: RequestHeaders };
  putFile: { body: WritePayload; headers?: RequestHeaders };
  deleteFile: { body: DeletePayload; headers?: RequestHeaders };
  getFile: { path: string; headers?: RequestHeaders };
  getFileInfo: { path: string; headers?: RequestHeaders };
  list: { prefix: string; headers?: RequestHeaders };
};

export type AuthorizeKind = keyof AuthorizePayload;

export type AuthorizeHook = <Kind extends AuthorizeKind>(
  kind: Kind,
  payload: AuthorizePayload[Kind],
) => void | Promise<void>;

export type PartitionSelector = <Kind extends AuthorizeKind>(
  payload: AuthorizePayload[Kind] & { kind: Kind },
) => unknown;

export type CreateWsfsApiOptions = {
  authorize?: AuthorizeHook;
  partition?: PartitionSelector;
};

type RequestContext = {
  headers?: RequestHeaders;
};

/** Canonical encoded file returned to callers. */
export type EncodedRecord = {
  path: string;
  etag: string;
  encoding: "utf8" | "base64";
  updatedBy?: string;
  /** Present when `encoding === "utf8"`. */
  content?: string;
  /** Present when `encoding === "base64"`. */
  contentBase64?: string;
};

/**
 * Payload shape for {@link WsfsBackendApi.sync}.
 *
 * - `prefix` scopes list + remote delta detection (defaults to "/").
 * - `writes` is an array of `{ path, content|contentBase64, encoding?, ifMatch? }`.
 *   - `ifMatch` should be `"*"` for new files or an existing etag for updates.
 *   - `contentBase64` can be used to stream binary data; `encoding` defaults to `"utf8"`.
 * - `deletes` is an array of `{ path, ifMatch? }`.
 * - `known` is the caller's view of remote state: `{ path, etag }` pairs used
 *   to calculate `remoteUpdates` and `remoteMissing`.
 * - `watermark` is an optional opaque cursor previously returned by the server
 *   to enable incremental sync when no changes occurred.
 *
 * Missing `writes`, `deletes`, and `known` are rejected with `BadRequestError`.
 */
export interface SyncRequestBody {
  prefix?: string;
  writes?: Array<WritePayload>;
  deletes?: Array<DeletePayload>;
  known?: Array<{
    path: string;
    etag: string | undefined;
  }>;
  watermark?: string;
  [key: string]: unknown;
}

/**
 * Response returned by {@link WsfsBackendApi.sync}.
 *
 * - `writeResults` mirrors each requested write:
 *   - `status` is `200` on success, `400` for bad input, `412`/`409` on etag mismatch.
 *   - `etag` is returned for successful writes.
 *   - `remoteEtag` shows the latest remote etag when a precondition fails.
 *   - `updatedBy` (optional) can be returned alongside `remoteEtag` for attribution.
 *   - `error` carries a human-readable reason on failure.
 * - `deleteResults` mirrors each requested delete with the same status/error pattern.
 *   - `status` is `204` on successful delete.
 * - `remoteUpdates` contains encoded file records that differ from the caller's `known` set.
 * - `remoteMissing` lists paths that were in `known` but are no longer present remotely.
 * - `watermark` echoes the latest mutation marker so callers can skip a full
 *   directory walk on the next sync.
 *
 * A single call can both mutate and fetch deltas, enabling efficient two-way sync.
 */
export interface SyncResponseBody {
  writeResults: Array<{
    path: string;
    status: number;
    etag?: string;
    remoteEtag?: string;
    updatedBy?: string;
    error?: string;
  }>;
  deleteResults: Array<{
    path: string;
    status: number;
    remoteEtag?: string;
    updatedBy?: string;
    error?: string;
  }>;
  remoteUpdates: EncodedRecord[];
  remoteMissing: string[];
  watermark: string | null;
}

export interface WsfsBackendApi {
  /**
   * Synchronize a batch of writes/deletes and fetch remote deltas.
   *
   * Conflicts:
   * - Writes/deletes honor `ifMatch`. If it does not match the remote etag,
   *   the item returns `status` 409/412 with `remoteEtag` populated.
   * - Missing paths inside the payload are treated as `400`.
   */
  sync(payload: SyncRequestBody, context?: RequestContext): Promise<SyncResponseBody>;
  /** Read full file contents if present. */
  getFile(path: string, context?: RequestContext): Promise<EncodedRecord | null>;
  /**
   * Read metadata only (etag + encoding).
   * Returns `null` when the file is missing.
   */
  getFileInfo(
    path: string,
    context?: RequestContext,
  ): Promise<{
    etag: string;
    encoding: "utf8" | "base64";
    updatedBy?: string;
  } | null>;
  /**
   * Create/update a file with optimistic concurrency via `ifMatch`.
   * Throws `BadRequestError` on missing `path`/content, and propagates
   * persistence errors (`MissingPreconditionError`, `EtagMismatchError`).
   */
  putFile(
    input: WritePayload,
    context?: RequestContext,
  ): Promise<{ etag: string }>;
  /**
   * Delete a file with optional `ifMatch` guard.
   * Throws `BadRequestError` when `path` is absent and propagates persistence errors.
   */
  deleteFile(input: DeletePayload, context?: RequestContext): Promise<void>;
  /**
   * List files under a prefix (defaults to "/").
   * Returns an array of `{ path, etag, encoding }` without content.
   */
  list(
    prefix?: string,
    context?: RequestContext,
  ): Promise<Array<Pick<FileRecord, "path" | "etag" | "encoding">>>;
}

/** Reusable core of the wsfs HTTP API, framework agnostic. */
export function createWsfsApi(
  persistence: PersistenceAdapter,
  options?: CreateWsfsApiOptions,
): WsfsBackendApi {
  return {
    sync: (payload, context) =>
      withHooks(
        persistence,
        options,
        "sync",
        { body: payload, headers: context?.headers },
        (scoped) => sync(scoped, payload),
      ),
    getFile: (path, context) =>
      withHooks(
        persistence,
        options,
        "getFile",
        { path, headers: context?.headers },
        (scoped) => getFile(scoped, path),
      ),
    getFileInfo: (path, context) =>
      withHooks(
        persistence,
        options,
        "getFileInfo",
        { path, headers: context?.headers },
        (scoped) => getFileInfo(scoped, path),
      ),
    putFile: (input, context) =>
      withHooks(
        persistence,
        options,
        "putFile",
        { body: input, headers: context?.headers },
        (scoped) => putFile(scoped, input),
      ),
    deleteFile: (input, context) =>
      withHooks(
        persistence,
        options,
        "deleteFile",
        { body: input, headers: context?.headers },
        (scoped) => deleteFile(scoped, input),
      ),
    list: (prefix, context) =>
      withHooks(
        persistence,
        options,
        "list",
        { prefix: prefix ?? "/", headers: context?.headers },
        (scoped) => list(scoped, prefix),
      ),
  };
}

type PartitionablePersistence = PersistenceAdapter & {
  withPartition(partition: unknown): PersistenceAdapter;
};

function hasPartition(
  adapter: PersistenceAdapter,
): adapter is PartitionablePersistence {
  return typeof (adapter as PartitionablePersistence).withPartition === "function";
}

async function withHooks<Kind extends AuthorizeKind, Result>(
  persistence: PersistenceAdapter,
  options: CreateWsfsApiOptions | undefined,
  kind: Kind,
  payload: AuthorizePayload[Kind],
  action: (scoped: PersistenceAdapter) => Promise<Result>,
): Promise<Result> {
  if (options?.authorize) {
    try {
      await options.authorize(kind, payload);
    } catch (err: unknown) {
      throw toAuthError(err);
    }
  }
  const partitionValue = options?.partition?.({ ...payload, kind } as AuthorizePayload[Kind] & {
    kind: Kind;
  });
  const scoped =
    partitionValue !== undefined && hasPartition(persistence)
      ? persistence.withPartition(partitionValue)
      : persistence;
  return action(scoped);
}

function toAuthError(err: unknown): Error & { status: number } {
  if (
    err &&
    typeof err === "object" &&
    "status" in err &&
    typeof (err as { status?: unknown }).status === "number"
  ) {
    const status = normalizeStatus((err as { status: number }).status);
    (err as { status: number }).status = status;
    return err as Error & { status: number };
  }
  const message =
    err instanceof Error
      ? err.message
      : typeof err === "string"
        ? err
        : "Unauthorized";
  const error = new Error(message) as Error & { status: number };
  error.status = 401;
  return error;
}

function normalizeStatus(status: number): number {
  return status === 400 || status === 401 || status === 403 ? status : 401;
}

async function sync(
  persistence: PersistenceAdapter,
  payload: SyncRequestBody,
): Promise<SyncResponseBody> {
  const prefix = payload.prefix || "/";
  const { writes, deletes, known } = payload;
  if (!writes && !deletes && !known) {
    throw new BadRequestError("No sync payload provided");
  }
  const writeResults: SyncResponseBody["writeResults"] = [];
  const deleteResults: SyncResponseBody["deleteResults"] = [];

  for (const file of writes || []) {
    const { path: targetPath, ifMatch } = file ?? {};
    if (!targetPath) {
      writeResults.push({
        path: targetPath,
        status: 400,
        error: "Missing path",
      });
      continue;
    }
    const decoded = decodeIncomingContent(file);
    if (!decoded) {
      writeResults.push({
        path: targetPath,
        status: 400,
        error: "Missing content",
      });
      continue;
    }
    try {
      const record = await persistence.write(targetPath, decoded.buffer, {
        ifMatch,
        encoding: decoded.encoding,
        updatedBy: file.updatedBy,
      });
      writeResults.push({ path: record.path, status: 200, etag: record.etag });
    } catch (err) {
      if (
        err instanceof MissingPreconditionError ||
        err instanceof EtagMismatchError
      ) {
        const remote = await persistence.read(targetPath);
        writeResults.push({
          path: targetPath,
          status: err.status,
          error: err.message,
          remoteEtag: remote?.etag,
          updatedBy: remote?.updatedBy,
        });
        continue;
      }
      throw err;
    }
  }

  for (const file of deletes || []) {
    const { path: targetPath, ifMatch } = file ?? {};
    if (!targetPath) {
      deleteResults.push({
        path: targetPath,
        status: 400,
        error: "Missing path",
      });
      continue;
    }
    try {
      await persistence.delete(targetPath, { ifMatch });
      deleteResults.push({ path: targetPath, status: 204 });
    } catch (err) {
      if (
        err instanceof MissingPreconditionError ||
        err instanceof EtagMismatchError
      ) {
        const remote = await persistence.read(targetPath);
        deleteResults.push({
          path: targetPath,
          status: err.status,
          error: err.message,
          remoteEtag: remote?.etag,
          updatedBy: remote?.updatedBy,
        });
        continue;
      }
      throw err;
    }
  }

  const knownMap = new Map((known || []).map((item) => [item.path, item.etag]));
  const shouldUseIncremental =
    !!persistence.listChanges && payload.watermark !== undefined;
  let remoteUpdates: EncodedRecord[] = [];
  const remoteMissing: string[] = [];
  let watermark: string | null = null;
  let incrementalHandled = false;

  if (shouldUseIncremental && persistence.listChanges) {
    const changes = await listAllChanges(
      persistence,
      prefix,
      payload.watermark,
    );
    if (!changes.reset) {
      watermark = changes.watermark ?? null;
      const toFetch: string[] = [];
      for (const item of changes.items) {
        if (item.deleted) {
          remoteMissing.push(item.path);
          continue;
        }
        if (knownMap.get(item.path) !== item.etag) {
          toFetch.push(item.path);
        }
      }
      remoteUpdates = await fetchMany(persistence, toFetch);
      incrementalHandled = true;
    }
  }

  if (!incrementalHandled) {
    const remoteList = await listAllPages(persistence, prefix);
    const remoteMap = new Map(remoteList.map((item) => [item.path, item.etag]));
    const toFetch: string[] = [];
    for (const item of remoteList) {
      if (knownMap.get(item.path) !== item.etag) {
        toFetch.push(item.path);
      }
    }
    remoteUpdates = await fetchMany(persistence, toFetch);
    for (const [pathItem] of knownMap.entries()) {
      if (!remoteMap.has(pathItem)) {
        remoteMissing.push(pathItem);
      }
    }
  }

  if (!watermark) {
    watermark = (await persistence.getWatermark?.(prefix)) ?? null;
  }

  return { writeResults, deleteResults, remoteUpdates, remoteMissing, watermark };
}

async function getFile(
  persistence: PersistenceAdapter,
  targetPath: string,
): Promise<EncodedRecord | null> {
  if (!targetPath) {
    throw new BadRequestError("Missing path");
  }
  const record = await persistence.read(targetPath);
  if (!record) {
    return null;
  }
  return encodeRecord(record);
}

async function getFileInfo(
  persistence: PersistenceAdapter,
  targetPath: string,
): Promise<{
  etag: string;
  encoding: "utf8" | "base64";
  updatedBy?: string;
} | null> {
  const record = await getFile(persistence, targetPath);
  if (!record) {
    return null;
  }
  return {
    etag: record.etag,
    encoding: record.encoding,
    updatedBy: record.updatedBy,
  };
}

async function putFile(
  persistence: PersistenceAdapter,
  input: WritePayload,
): Promise<{ etag: string }> {
  const { path: targetPath } = input;
  if (!targetPath) {
    throw new BadRequestError("Missing path");
  }
  const payload = decodeIncomingContent(input);
  if (!payload) {
    throw new BadRequestError("Missing content");
  }
  const record = await persistence.write(targetPath, payload.buffer, {
    ifMatch: input.ifMatch,
    encoding: payload.encoding,
    updatedBy: input.updatedBy,
  });
  return { etag: record.etag };
}

async function deleteFile(
  persistence: PersistenceAdapter,
  input: DeletePayload,
): Promise<void> {
  const { path: targetPath } = input;
  if (!targetPath) {
    throw new BadRequestError("Missing path");
  }
  await persistence.delete(targetPath, { ifMatch: input.ifMatch });
}

async function list(
  persistence: PersistenceAdapter,
  prefix = "/",
): Promise<Array<Pick<FileRecord, "path" | "etag" | "encoding">>> {
  return listAllPages(persistence, prefix);
}

async function listAllPages(
  persistence: PersistenceAdapter,
  prefix: string,
): Promise<Array<Pick<FileRecord, "path" | "etag" | "encoding">>> {
  const collected: Array<Pick<FileRecord, "path" | "etag" | "encoding">> = [];
  let cursor: string | undefined;
  do {
    const page = normalizeListResult(await persistence.list(prefix, undefined, cursor));
    collected.push(...page.items);
    cursor = page.cursor ?? undefined;
  } while (cursor);
  return collected;
}

async function listAllChanges(
  persistence: PersistenceAdapter,
  prefix: string,
  watermark?: string,
): Promise<{ items: ListChangeItem[]; watermark: string | null; reset: boolean }> {
  if (!persistence.listChanges) {
    return { items: [], watermark: null, reset: true };
  }
  const collected: ListChangeItem[] = [];
  let cursor: string | undefined;
  let latestWatermark: string | null = null;
  do {
    const page = await persistence.listChanges(prefix, watermark, undefined, cursor);
    if (page.reset) {
      return {
        items: [],
        watermark: page.watermark ?? latestWatermark ?? null,
        reset: true,
      };
    }
    collected.push(...page.items);
    if (page.watermark !== undefined && page.watermark !== null) {
      latestWatermark = page.watermark;
    }
    cursor = page.cursor ?? undefined;
  } while (cursor);
  return { items: collected, watermark: latestWatermark ?? null, reset: false };
}

async function fetchMany(
  persistence: PersistenceAdapter,
  paths: string[],
): Promise<EncodedRecord[]> {
  if (!paths.length) {
    return [];
  }
  if (persistence.readMany) {
    const records = await persistence.readMany(paths);
    return records
      .map((record) => (record ? encodeRecord(record) : null))
      .filter((record): record is EncodedRecord => !!record);
  }
  const records = await Promise.all(paths.map((pathItem) => persistence.read(pathItem)));
  return records
    .map((record) => (record ? encodeRecord(record) : null))
    .filter((record): record is EncodedRecord => !!record);
}

function normalizeListResult(
  result: Awaited<ReturnType<PersistenceAdapter["list"]>>,
): { items: Array<Pick<FileRecord, "path" | "etag" | "encoding">>; cursor: string | null } {
  if (Array.isArray(result)) {
    return { items: result, cursor: null };
  }
  return {
    items: result.items,
    cursor: result.cursor ?? null,
  };
}

function decodeIncomingContent(
  body: IncomingContent | null | undefined,
): { buffer: Buffer; encoding: "utf8" | "base64" } | null {
  if (!body) return null;
  if (typeof body.contentBase64 === "string" || body.encoding === "base64") {
    const base64 = body.contentBase64 ?? body.content;
    if (typeof base64 !== "string") {
      return null;
    }
    return { buffer: Buffer.from(base64, "base64"), encoding: "base64" };
  }
  if (typeof body.content === "string" || Buffer.isBuffer(body.content)) {
    return {
      buffer: Buffer.isBuffer(body.content)
        ? body.content
        : Buffer.from(body.content, "utf8"),
      encoding: "utf8",
    };
  }
  if (body.content !== undefined) {
    return {
      buffer: Buffer.from(JSON.stringify(body.content), "utf8"),
      encoding: "utf8",
    };
  }
  return null;
}

function encodeRecord(record: FileRecord): EncodedRecord {
  if (record.encoding === "utf8") {
    return {
      path: record.path,
      etag: record.etag,
      encoding: "utf8",
      updatedBy: record.updatedBy,
      content: record.content.toString("utf8"),
    };
  }
  return {
    path: record.path,
    etag: record.etag,
    encoding: "base64",
    updatedBy: record.updatedBy,
    contentBase64: record.content.toString("base64"),
  };
}
