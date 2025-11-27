import { createHash } from "node:crypto";
import path from "node:path";
import {
  EtagMismatchError,
  FileRecord,
  ListChangeItem,
  ListChangesPage,
  MissingPreconditionError,
  PersistenceAdapter,
} from "./persistence.js";

type SqlExecutor = {
  /** Fetch a single row or undefined. */
  get<T = unknown>(sql: string, params: unknown[]): Promise<T | undefined>;
  /** Fetch all rows. */
  all<T = unknown>(sql: string, params: unknown[]): Promise<T[]>;
  /** Run a mutation. Return rowsAffected when available. */
  run(
    sql: string,
    params: unknown[],
  ): Promise<{ rowsAffected?: number } | void>;
  /**
   * Optional transaction wrapper. Implementations should run the callback in a
   * single transaction and commit/rollback automatically.
   */
  transaction?<T>(callback: () => Promise<T>): Promise<T>;
};

type PartitionBinder<Partition> = {
  /** Columns that scope every query (e.g., user_id, vault_id). */
  columns: string[];
  /** Convert a partition object into ordered parameter values matching columns. */
  toParams: (partition: Partition) => unknown[];
};

type SqlPersistenceOptions<Partition> = {
  /** Table storing the latest file state. */
  table: string;
  /** Change log table storing tombstones and watermarks. */
  changesTable: string;
  /** Bindings for multi-tenant partition columns. */
  partition: PartitionBinder<Partition>;
  /** Bound partition values (e.g., { userId, vaultId }). */
  partitionValue: Partition;
  /** Executor over your DB client (SQLite, Postgres, etc.). */
  executor: SqlExecutor;
  /** Optional clock for tests. Defaults to Date.now. */
  now?: () => number;
  /** Optional etag generator. Defaults to sha256 of content. */
  computeEtag?: (content: Buffer) => string;
  /** Optional encoding detector. Defaults to utf8-or-base64 heuristic. */
  detectEncoding?: (content: Buffer) => "utf8" | "base64";
};

type FileRow = {
  path: string;
  etag: string;
  encoding: "utf8" | "base64";
  content: Buffer;
  updated_by?: string | null;
  updated_at: number;
  deleted?: number;
};

type ChangeRow = {
  path: string;
  etag: string | null;
  encoding: "utf8" | "base64" | null;
  deleted: number;
  updated_by?: string | null;
  updated_at: number;
};

/**
 * Generic SQL persistence adapter with partition columns and incremental
 * watermarks. Works with any driver that supports positional `?` parameters
 * (SQLite, better-sqlite3, mysql2); for Postgres, pass an executor that
 * rewrites placeholders or uses a query builder.
 *
 * Assumes these tables (column names configurable via `partition.columns`):
 * - files: (partition cols...), path (PK), etag, encoding, content BLOB,
 *   updated_by NULL, updated_at INTEGER
 * - file_changes: same partition cols + path, etag, encoding, deleted BOOLEAN,
 *   updated_by NULL, updated_at INTEGER
 */
export class SqlPersistence<Partition> implements PersistenceAdapter {
  private readonly table: string;
  private readonly changesTable: string;
  private readonly executor: SqlExecutor;
  private readonly partition: PartitionBinder<Partition>;
  private readonly partitionValue: Partition;
  private readonly now: () => number;
  private readonly computeEtag: (content: Buffer) => string;
  private readonly detectEncoding: (content: Buffer) => "utf8" | "base64";

  constructor(options: SqlPersistenceOptions<Partition>) {
    this.table = options.table;
    this.changesTable = options.changesTable;
    this.executor = options.executor;
    this.partition = options.partition;
    this.partitionValue = options.partitionValue;
    this.now = options.now ?? Date.now;
    this.computeEtag =
      options.computeEtag ??
      ((content: Buffer) => createHash("sha256").update(content).digest("hex"));
    this.detectEncoding =
      options.detectEncoding ?? ((content: Buffer) => this.defaultDetectEncoding(content));
  }

  withPartition(partition: Partition): SqlPersistence<Partition> {
    return new SqlPersistence({
      table: this.table,
      changesTable: this.changesTable,
      executor: this.executor,
      partition: this.partition,
      partitionValue: partition,
      now: this.now,
      computeEtag: this.computeEtag,
      detectEncoding: this.detectEncoding,
    });
  }

  async read(targetPath: string): Promise<FileRecord | null> {
    const normalizedPath = this.normalizePath(targetPath);
    const row = await this.executor.get<FileRow>(
      `SELECT path, etag, encoding, content, updated_by, updated_at
       FROM ${this.table}
       WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}
         AND path = ?
         AND deleted IS NOT 1`,
      [...this.partitionParams(), normalizedPath],
    );
    if (!row) {
      return null;
    }
    return this.rowToRecord(row);
  }

  async readMany(paths: string[]): Promise<Array<FileRecord | null>> {
    if (!paths.length) return [];
    const normalized = paths.map((p) => this.normalizePath(p));
    const placeholders = normalized.map(() => "?").join(", ");
    const rows = await this.executor.all<FileRow>(
      `SELECT path, etag, encoding, content, updated_by, updated_at
       FROM ${this.table}
       WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}
         AND path IN (${placeholders})
         AND deleted IS NOT 1`,
      [...this.partitionParams(), ...normalized],
    );
    const map = new Map(rows.map((row) => [row.path, this.rowToRecord(row)]));
    return normalized.map((p) => map.get(p) ?? null);
  }

  async write(
    targetPath: string,
    content: string | Buffer,
    options: {
      ifMatch?: string;
      encoding?: "utf8" | "base64";
      updatedBy?: string;
    },
  ): Promise<FileRecord> {
    if (!options.ifMatch) {
      throw new MissingPreconditionError("If-Match header is required");
    }
    const normalizedPath = this.normalizePath(targetPath);
    const buffer = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const encoding = options.encoding ?? this.detectEncoding(buffer);
    const etag = this.computeEtag(buffer);
    const now = this.now();
    const params = this.partitionParams();

    const exec = async (): Promise<FileRecord> => {
      const existing = await this.executor.get<FileRow>(
        `SELECT etag FROM ${this.table}
         WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}
           AND path = ?
           AND deleted IS NOT 1`,
        [...params, normalizedPath],
      );
      if (!existing && options.ifMatch !== "*") {
        throw new EtagMismatchError("File does not exist for provided ETag");
      }
      if (existing && existing.etag !== options.ifMatch) {
        throw new EtagMismatchError("ETag mismatch");
      }
      const mutationParams = [
        ...params,
        normalizedPath,
        etag,
        encoding,
        buffer,
        options.updatedBy ?? null,
        now,
      ];

      const upsertSql = existing
        ? `UPDATE ${this.table}
             SET etag = ?, encoding = ?, content = ?, updated_by = ?, updated_at = ?, deleted = 0
             WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}
               AND path = ?
               AND etag = ?`
        : `INSERT INTO ${this.table} (${[
            ...this.partition.columns,
            "path",
            "etag",
            "encoding",
            "content",
            "updated_by",
            "updated_at",
            "deleted",
          ].join(", ")})
           VALUES (${[
             ...this.partition.columns.map(() => "?"),
             "?", // path
             "?", // etag
             "?", // encoding
             "?", // content
             "?", // updated_by
             "?", // updated_at
             "0", // deleted
           ].join(", ")})`;

      if (existing) {
        const result = await this.executor.run(upsertSql, [
          etag,
          encoding,
          buffer,
          options.updatedBy ?? null,
          now,
          ...params,
          normalizedPath,
          options.ifMatch,
        ]);
        if (result && result.rowsAffected === 0) {
          throw new EtagMismatchError("ETag mismatch");
        }
      } else {
        await this.executor.run(upsertSql, mutationParams);
      }

      await this.recordChange({
        partition: this.partitionValue,
        path: normalizedPath,
        etag,
        encoding,
        deleted: false,
        updatedBy: options.updatedBy,
        updatedAt: now,
      });

      return {
        path: normalizedPath,
        etag,
        encoding,
        updatedBy: options.updatedBy,
        content: Buffer.from(buffer),
      };
    };

    if (this.executor.transaction) {
      return this.executor.transaction(exec);
    }
    return exec();
  }

  async delete(
    targetPath: string,
    options: {
      ifMatch?: string;
    },
  ): Promise<void> {
    if (!options.ifMatch) {
      throw new MissingPreconditionError("If-Match header is required");
    }
    const normalizedPath = this.normalizePath(targetPath);
    const params = this.partitionParams();
    const now = this.now();

    const exec = async (): Promise<void> => {
      const existing = await this.executor.get<FileRow>(
        `SELECT etag, encoding, updated_by FROM ${this.table}
         WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}
           AND path = ?
           AND deleted IS NOT 1`,
        [...params, normalizedPath],
      );
      if (!existing && options.ifMatch !== "*") {
        throw new EtagMismatchError("File does not exist for provided ETag");
      }
      if (existing && existing.etag !== options.ifMatch) {
        throw new EtagMismatchError("ETag mismatch");
      }

      await this.executor.run(
        `UPDATE ${this.table}
         SET deleted = 1, updated_at = ?
         WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}
           AND path = ?`,
        [now, ...params, normalizedPath],
      );

      await this.recordChange({
        partition: this.partitionValue,
        path: normalizedPath,
        etag: existing?.etag ?? null,
        encoding: existing?.encoding ?? null,
        deleted: true,
        updatedBy: existing?.updated_by ?? undefined,
        updatedAt: now,
      });
    };

    if (this.executor.transaction) {
      await this.executor.transaction(exec);
    } else {
      await exec();
    }
  }

  async list(
    prefix = "/",
    limit?: number,
    cursor?: string,
  ): Promise<
    Array<Pick<FileRecord, "path" | "etag" | "encoding">> | {
      items: Array<Pick<FileRecord, "path" | "etag" | "encoding">>;
      cursor?: string | null;
    }
  > {
    const normalizedPrefix = this.normalizePath(prefix).replace(/\/+$/, "");
    const params = this.partitionParams();
    const whereParts = [
      ...this.partition.columns.map((col) => `${col} = ?`),
      "deleted IS NOT 1",
      normalizedPrefix === "/" ? "path LIKE '/%'" : "path LIKE ? ESCAPE '\\'",
    ];
    const sqlParts = [
      `SELECT path, etag, encoding FROM ${this.table}`,
      `WHERE ${whereParts.join(" AND ")}`,
    ];
    if (limit !== undefined) {
      sqlParts.push("ORDER BY path");
      sqlParts.push("LIMIT ?");
      if (cursor) {
        sqlParts.push("OFFSET ?");
      }
    }

    const queryParams = [...params];
    if (normalizedPrefix !== "/") {
      queryParams.push(this.prefixLike(normalizedPrefix));
    }
    if (limit !== undefined) {
      queryParams.push(limit);
      if (cursor) {
        queryParams.push(Number(cursor));
      }
    }

    const rows = await this.executor.all<FileRow>(sqlParts.join(" "), queryParams);
    if (limit === undefined) {
      return rows.map((row) => ({
        path: row.path,
        etag: row.etag,
        encoding: row.encoding,
      }));
    }
    const nextCursor = rows.length === limit ? String((cursor ? Number(cursor) : 0) + rows.length) : null;
    return {
      items: rows.map((row) => ({
        path: row.path,
        etag: row.etag,
        encoding: row.encoding,
      })),
      cursor: nextCursor,
    };
  }

  async listChanges(
    prefix = "/",
    since?: string,
    limit?: number,
    cursor?: string,
  ): Promise<ListChangesPage> {
    const normalizedPrefix = this.normalizePath(prefix).replace(/\/+$/, "");
    const sinceClock = since ? Number(since) : 0;
    const params = this.partitionParams();

    const maxRow = await this.executor.get<{ maxClock: number | null }>(
      `SELECT MAX(updated_at) as maxClock
       FROM ${this.changesTable}
       WHERE ${this.partition.columns.map((col) => `${col} = ?`).join(" AND ")}`,
      params,
    );
    const maxClock = maxRow?.maxClock ?? 0;
    if (sinceClock > maxClock) {
      return { items: [], cursor: null, watermark: String(maxClock), reset: true };
    }

    const whereParts = [
      ...this.partition.columns.map((col) => `${col} = ?`),
      "updated_at > ?",
    ];
    whereParts.push(normalizedPrefix === "/" ? "path LIKE '/%'" : "path LIKE ? ESCAPE '\\'");

    const sqlParts = [
      `SELECT path, etag, encoding, deleted, updated_by, updated_at
       FROM ${this.changesTable}`,
      `WHERE ${whereParts.join(" AND ")}`,
      "ORDER BY updated_at",
    ];
    if (limit !== undefined) {
      sqlParts.push("LIMIT ?");
      if (cursor) {
        sqlParts.push("OFFSET ?");
      }
    }

    const queryParams = [...params, sinceClock];
    if (normalizedPrefix !== "/") {
      queryParams.push(this.prefixLike(normalizedPrefix));
    }
    if (limit !== undefined) {
      queryParams.push(limit);
      if (cursor) {
        queryParams.push(Number(cursor));
      }
    }

    const rows = await this.executor.all<ChangeRow>(sqlParts.join(" "), queryParams);
    const items: ListChangeItem[] = rows.map((row) => ({
      path: row.path,
      etag: row.etag ?? "",
      encoding: (row.encoding as "utf8" | "base64") ?? "utf8",
      deleted: row.deleted === 1,
    }));
    const latestWatermark = rows.length ? rows[rows.length - 1].updated_at : maxClock;
    const nextCursor = limit !== undefined && rows.length === limit
      ? String((cursor ? Number(cursor) : 0) + rows.length)
      : null;
    return {
      items,
      cursor: nextCursor,
      watermark: String(latestWatermark),
      reset: false,
    };
  }

  async getWatermark(prefix = "/"): Promise<string | null> {
    const normalizedPrefix = this.normalizePath(prefix).replace(/\/+$/, "");
    const params = this.partitionParams();
    const whereParts = [
      ...this.partition.columns.map((col) => `${col} = ?`),
      normalizedPrefix === "/" ? "path LIKE '/%'" : "path LIKE ? ESCAPE '\\'",
    ];
    const row = await this.executor.get<{ watermark: number | null }>(
      `SELECT MAX(updated_at) as watermark
       FROM ${this.changesTable}
       WHERE ${whereParts.join(" AND ")}`,
      normalizedPrefix === "/" ? params : [...params, this.prefixLike(normalizedPrefix)],
    );
    return row?.watermark !== null && row?.watermark !== undefined
      ? String(row.watermark)
      : null;
  }

  private rowToRecord(row: FileRow): FileRecord {
    return {
      path: row.path,
      etag: row.etag,
      encoding: row.encoding,
      updatedBy: row.updated_by ?? undefined,
      content: Buffer.from(row.content),
    };
  }

  private async recordChange(input: {
    partition: Partition;
    path: string;
    etag: string | null;
    encoding: "utf8" | "base64" | null;
    deleted: boolean;
    updatedBy?: string;
    updatedAt: number;
  }): Promise<void> {
    const params = [
      ...this.partition.toParams(input.partition),
      input.path,
      input.etag,
      input.encoding,
      input.deleted ? 1 : 0,
      input.updatedBy ?? null,
      input.updatedAt,
    ];
    await this.executor.run(
      `INSERT INTO ${this.changesTable} (${[
        ...this.partition.columns,
        "path",
        "etag",
        "encoding",
        "deleted",
        "updated_by",
        "updated_at",
      ].join(", ")})
       VALUES (${[
         ...this.partition.columns.map(() => "?"),
         "?", // path
         "?", // etag
         "?", // encoding
         "?", // deleted
         "?", // updated_by
         "?", // updated_at
       ].join(", ")})`,
      params,
    );
  }

  private prefixLike(prefix: string): string {
    const escaped = prefix.replace(/([_%\\\\])/g, "\\$1");
    return `${escaped}/%`;
  }

  private partitionParams(): unknown[] {
    if (this.partitionValue === undefined || this.partitionValue === null) {
      throw new Error("Partition value must be provided");
    }
    return this.partition.toParams(this.partitionValue);
  }

  private normalizePath(targetPath: string): string {
    if (!targetPath || !targetPath.startsWith("/")) {
      throw new Error("Paths must start with '/'");
    }
    if (targetPath.includes("..")) {
      throw new Error("Parent directory traversal is not allowed");
    }
    if (targetPath.includes("\0")) {
      throw new Error("Null bytes are not allowed in paths");
    }
    const normalized = path.posix.normalize(targetPath);
    if (!normalized.startsWith("/")) {
      throw new Error("Paths must start with '/'");
    }
    return normalized === "/" ? normalized : normalized.replace(/\/+$/, "");
  }

  private defaultDetectEncoding(content: Buffer): "utf8" | "base64" {
    const asUtf8 = content.toString("utf8");
    const roundTrip = Buffer.from(asUtf8, "utf8");
    return roundTrip.length === content.length && roundTrip.equals(content)
      ? "utf8"
      : "base64";
  }
}
