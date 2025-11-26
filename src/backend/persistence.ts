import { Buffer } from "node:buffer";

/** Opaque record stored in the backing persistence layer. */
export interface FileRecord {
  /** normalized absolute path */
  path: string;
  /** backend-managed version */
  etag: string;
  /** optional author metadata */
  updatedBy?: string;
  /** Raw file bytes */
  content: Buffer;
  /** Encoding provided by the writer (utf8/base64) */
  encoding: "utf8" | "base64";
}

export type ListResultItem = Pick<FileRecord, "path" | "etag" | "encoding">;

export type ListPage = {
  items: ListResultItem[];
  cursor?: string | null;
};

export type ListChangeItem = ListResultItem & {
  deleted?: boolean;
};

export type ListChangesPage = {
  items: ListChangeItem[];
  cursor?: string | null;
  watermark?: string | null;
  /** Signal to callers that the `since` token could not be honored. */
  reset?: boolean;
};

/** Contract the server uses to interact with storage. */
export interface PersistenceAdapter {
  /** Read a file or return null if missing. */
  read(path: string): Promise<FileRecord | null>;
  /** Optional bulk read to avoid N+1 calls when fetching multiple files. */
  readMany?(paths: string[]): Promise<Array<FileRecord | null>>;
  /** Create or update a file, honoring If-Match semantics. */
  write(
    path: string,
    content: string | Buffer,
    options: {
      ifMatch?: string;
      encoding?: "utf8" | "base64";
      updatedBy?: string;
    },
  ): Promise<FileRecord>;
  /** Delete a file, honoring If-Match semantics. */
  delete(
    path: string,
    options: {
      ifMatch?: string;
    },
  ): Promise<void>;
  /**
   * List files under a prefix with their etags (no content).
   *
   * When `limit` is provided, implementations may return a partial page plus a
   * cursor for the next page. Without `limit`, returning the full list remains
   * valid for backward compatibility.
   */
  list(
    prefix: string,
    limit?: number,
    cursor?: string,
  ): Promise<Array<ListResultItem> | ListPage>;
  /**
   * Optional incremental listing that returns only records changed since a
   * caller-provided watermark. Implementations should surface `reset: true`
   * when the provided watermark cannot be honored so callers can fall back to a
   * full list.
   */
  listChanges?(
    prefix: string,
    since?: string,
    limit?: number,
    cursor?: string,
  ): Promise<ListChangesPage>;
  /** Latest known watermark for the prefix, if tracking is available. */
  getWatermark?(prefix: string): Promise<string | null>;
}

export class EtagMismatchError extends Error {
  status: number;
  /** Raised when If-Match does not align with stored etag. */
  constructor(message = "ETag mismatch") {
    super(message);
    this.status = 412;
  }
}

export class MissingPreconditionError extends Error {
  status: number;
  /** Raised when required headers (e.g., If-Match) are absent. */
  constructor(message = "Missing required precondition") {
    super(message);
    this.status = 428;
  }
}
