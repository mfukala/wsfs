import express, { type Express, type Request, type Response } from "express";
import type { PersistenceAdapter } from "../src/backend/persistence.js";
import {
  type AuthorizeHook,
  type PartitionSelector,
  BadRequestError,
  createWsfsApi,
} from "../src/backend/backendApi.js";

type ServerOptions = {
  persistence: PersistenceAdapter;
  authorize?: AuthorizeHook;
  partition?: PartitionSelector;
};

/** Minimal Express server used only for tests. */
export function createWsfsServer(options: ServerOptions): Express {
  const api = createWsfsApi(options.persistence, {
    authorize: options.authorize,
    partition: options.partition,
  });
  const app = express();
  app.use(express.json({ limit: "5mb" }));

  const toHttpError = (err: unknown, fallback: string) => {
    const status =
      err && typeof err === "object" && "status" in err && typeof err.status === "number"
        ? err.status
        : 500;
    const message =
      err instanceof Error
        ? err.message
        : err && typeof err === "object" && "message" in err && typeof err.message === "string"
          ? err.message
          : fallback;
    return { status, message };
  };
  const ensurePathArray = (value: unknown): string[] => {
    if (!Array.isArray(value) || value.length === 0) {
      throw new BadRequestError("No paths provided");
    }
    for (const path of value) {
      if (typeof path !== "string" || !path) {
        throw new BadRequestError("Invalid path");
      }
    }
    return value;
  };

  app.post("/sync", async (req: Request, res: Response) => {
    try {
      const result = await api.sync(req.body, { headers: req.headers });
      res.status(200).json(result);
    } catch (err: unknown) {
      const { status, message } = toHttpError(err, "sync failed");
      res.status(status).json({ error: message });
    }
  });

  app.get("/file", async (req: Request, res: Response) => {
    const path = req.query.path as string | undefined;
    try {
      const result = await api.getFile(path ?? "", { headers: req.headers });
      if (!result) {
        res.status(404).end();
        return;
      }
      res.status(200).json(result);
    } catch (err: unknown) {
      const { status, message } = toHttpError(err, "getFile failed");
      res.status(status).json({ error: message });
    }
  });

  app.get("/file/info", async (req: Request, res: Response) => {
    const path = req.query.path as string | undefined;
    try {
      const result = await api.getFileInfo(path ?? "", { headers: req.headers });
      if (!result) {
        res.status(404).end();
        return;
      }
      res.status(200).json(result);
    } catch (err: unknown) {
      const { status, message } = toHttpError(err, "getFileInfo failed");
      res.status(status).json({ error: message });
    }
  });

  app.post("/file/batch", async (req: Request, res: Response) => {
    try {
      const paths = ensurePathArray(req.body?.paths);
      const result = await api.getFiles(paths, { headers: req.headers });
      res.status(200).json(result);
    } catch (err: unknown) {
      const { status, message } = toHttpError(err, "getFiles failed");
      res.status(status).json({ error: message });
    }
  });

  app.post("/file/info/batch", async (req: Request, res: Response) => {
    try {
      const paths = ensurePathArray(req.body?.paths);
      const result = await api.getFileInfos(paths, { headers: req.headers });
      res.status(200).json(result);
    } catch (err: unknown) {
      const { status, message } = toHttpError(err, "getFileInfos failed");
      res.status(status).json({ error: message });
    }
  });

  return app;
}
