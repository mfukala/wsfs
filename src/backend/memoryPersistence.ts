import { createHash } from "node:crypto";
import path from "node:path";
import {
  EtagMismatchError,
  FileRecord,
  ListChangesPage,
  MissingPreconditionError,
  PersistenceAdapter,
} from "./persistence.js";

type ChangeMeta = {
  etag?: string;
  encoding?: "utf8" | "base64";
  deleted?: boolean;
  updatedBy?: string;
  updatedAt: number;
};

/** In-memory PersistenceAdapter for tests; exposes stored records directly. */
export class MemoryPersistence implements PersistenceAdapter {
  /** Backing store keyed by normalized path. */
  readonly records: Map<string, FileRecord>;
  /** Track the last mutation clock for incremental sync. */
  private watermarkCounter: number;
  /**
   * Change log entries keyed by path. Deleted entries remain here so
   * listChanges can emit tombstones.
   */
  private changes: Map<string, ChangeMeta>;

  constructor() {
    this.records = new Map();
    this.watermarkCounter = 0;
    this.changes = new Map();
  }

  async read(targetPath: string): Promise<FileRecord | null> {
    const normalizedPath = this.normalizePath(targetPath);
    const existing = this.records.get(normalizedPath);
    if (!existing) {
      return null;
    }
    return {
      path: existing.path,
      etag: existing.etag,
      encoding: existing.encoding,
      updatedBy: existing.updatedBy,
      content: Buffer.from(existing.content),
    };
  }

  async readMany(paths: string[]): Promise<Array<FileRecord | null>> {
    return Promise.all(paths.map((pathItem) => this.read(pathItem)));
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
    const existing = this.records.get(normalizedPath);
    const buffer = Buffer.isBuffer(content) ? content : Buffer.from(content);
    const encoding = options.encoding ?? this.detectEncoding(buffer);

    if (!existing && options.ifMatch !== "*") {
      throw new EtagMismatchError("File does not exist for provided ETag");
    }
    if (existing && existing.etag !== options.ifMatch) {
      throw new EtagMismatchError("ETag mismatch");
    }

    const etag = this.computeEtag(buffer);
    const record: FileRecord = {
      path: normalizedPath,
      etag,
      content: Buffer.from(buffer),
      encoding,
      updatedBy: options.updatedBy,
    };
    this.records.set(normalizedPath, record);
    this.recordChange(normalizedPath, {
      etag,
      encoding,
      deleted: false,
      updatedBy: options.updatedBy,
    });
    return { ...record, content: Buffer.from(record.content) };
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
    const existing = this.records.get(normalizedPath);
    if (!existing && options.ifMatch !== "*") {
      throw new EtagMismatchError("File does not exist for provided ETag");
    }
    if (existing && existing.etag !== options.ifMatch) {
      throw new EtagMismatchError("ETag mismatch");
    }
    this.records.delete(normalizedPath);
    this.recordChange(normalizedPath, {
      etag: existing?.etag,
      encoding: existing?.encoding,
      deleted: true,
      updatedBy: existing?.updatedBy,
    });
  }

  async list(
    prefix: string,
    limit?: number,
    cursor?: string,
  ): Promise<Array<Pick<FileRecord, "path" | "etag" | "encoding">> | {
    items: Array<Pick<FileRecord, "path" | "etag" | "encoding">>;
    cursor?: string | null;
  }> {
    const normalizedPrefix = this.normalizePath(prefix).replace(/\/+$/, "");
    const results: Array<Pick<FileRecord, "path" | "etag" | "encoding">> = [];
    for (const record of this.records.values()) {
      if (!record.path.startsWith(normalizedPrefix)) {
        continue;
      }
      results.push({
        path: record.path,
        etag: record.etag,
        encoding: record.encoding,
      });
    }
    if (!limit) {
      return results;
    }
    const offset = cursor ? Number(cursor) : 0;
    const items = results.slice(offset, offset + limit);
    const nextOffset = offset + items.length;
    return {
      items,
      cursor: nextOffset < results.length ? String(nextOffset) : null,
    };
  }

  async listChanges(
    prefix: string,
    since?: string,
    limit?: number,
    cursor?: string,
  ): Promise<ListChangesPage> {
    const normalizedPrefix = this.normalizePath(prefix).replace(/\/+$/, "");
    const sinceClock = since ? Number(since) : 0;
    if (sinceClock > this.watermarkCounter) {
      return {
        items: [],
        cursor: null,
        watermark: String(this.watermarkCounter),
        reset: true,
      };
    }
    const sorted = [...this.changes.entries()]
      .filter(
        ([pathItem, meta]) =>
          pathItem.startsWith(normalizedPrefix) && meta.updatedAt > sinceClock,
      )
      .sort((a, b) => a[1].updatedAt - b[1].updatedAt);
    const offset = limit ? Number(cursor ?? 0) : 0;
    const sliceEnd = limit ? offset + limit : undefined;
    const window = sorted.slice(offset, sliceEnd);
    const items = window.map(([pathItem, meta]) => ({
      path: pathItem,
      etag: meta.etag ?? "",
      encoding: meta.encoding ?? "utf8",
      deleted: !!meta.deleted,
    }));
    const nextCursor =
      limit && offset + items.length < sorted.length
        ? String(offset + items.length)
        : null;
    return {
      items,
      cursor: nextCursor,
      watermark: String(this.watermarkCounter),
      reset: false,
    };
  }

  async getWatermark(): Promise<string | null> {
    return String(this.watermarkCounter);
  }

  private computeEtag(content: Buffer): string {
    return createHash("sha256").update(content).digest("hex");
  }

  private normalizePath(targetPath: string): string {
    if (!targetPath.startsWith("/")) {
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

  private detectEncoding(content: Buffer): "utf8" | "base64" {
    const asUtf8 = content.toString("utf8");
    const roundTrip = Buffer.from(asUtf8, "utf8");
    return roundTrip.length === content.length && roundTrip.equals(content)
      ? "utf8"
      : "base64";
  }

  private recordChange(
    pathItem: string,
    meta: Omit<ChangeMeta, "updatedAt">,
  ): void {
    this.watermarkCounter += 1;
    this.changes.set(pathItem, {
      ...meta,
      updatedAt: this.watermarkCounter,
    });
  }
}
