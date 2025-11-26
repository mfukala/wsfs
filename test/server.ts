import express, { type Express, type Request, type Response } from "express";
import type { PersistenceAdapter } from "../src/backend/persistence.js";
import { createWsfsApi } from "../src/backend/backendApi.js";

type ServerOptions = {
  persistence: PersistenceAdapter;
};

/** Minimal Express server used only for tests. */
export function createWsfsServer(options: ServerOptions): Express {
  const api = createWsfsApi(options.persistence);
  const app = express();
  app.use(express.json({ limit: "5mb" }));

  app.post("/sync", async (req: Request, res: Response) => {
    try {
      const result = await api.sync(req.body);
      res.status(200).json(result);
    } catch (err: any) {
      res.status(err?.status ?? 500).json({ error: err?.message ?? "sync failed" });
    }
  });

  app.get("/file", async (req: Request, res: Response) => {
    const path = req.query.path as string | undefined;
    try {
      const result = await api.getFile(path ?? "");
      if (!result) {
        res.status(404).end();
        return;
      }
      res.status(200).json(result);
    } catch (err: any) {
      res.status(err?.status ?? 500).json({ error: err?.message ?? "getFile failed" });
    }
  });

  app.get("/file/info", async (req: Request, res: Response) => {
    const path = req.query.path as string | undefined;
    try {
      const result = await api.getFileInfo(path ?? "");
      if (!result) {
        res.status(404).end();
        return;
      }
      res.status(200).json(result);
    } catch (err: any) {
      res
        .status(err?.status ?? 500)
        .json({ error: err?.message ?? "getFileInfo failed" });
    }
  });

  return app;
}
