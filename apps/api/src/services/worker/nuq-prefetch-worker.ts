import "dotenv/config";
import { config } from "../../config";
import "../sentry";
import { setSentryServiceTag } from "../sentry";
import {
  scrapeQueue,
  nuqGetLocalMetrics,
  nuqHealthCheck,
  nuqShutdown,
  crawlFinishedQueue,
} from "./nuq";
import Express from "express";
import { logger } from "../../lib/logger";

(async () => {
  setSentryServiceTag("nuq-prefetch-worker");

  const app = Express();

  app.get("/metrics", (_, res) =>
    res.contentType("text/plain").send(nuqGetLocalMetrics()),
  );
  app.get("/health", async (_, res) => {
    if (await nuqHealthCheck()) {
      res.status(200).send("OK");
    } else {
      res.status(500).send("Not OK");
    }
  });

  const server = app.listen(config.NUQ_PREFETCH_WORKER_PORT, () => {
    logger.info("NuQ prefetch worker metrics server started");
  });

  async function shutdown() {
    server.close();
    await nuqShutdown();
    process.exit(0);
  }

  if (require.main === module) {
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  }

  const MAX_IDLE_INTERVAL = 10000;
  const FAST_INTERVAL = 250;

  const prefetchLoop = async (queue: { prefetchJobs: () => Promise<number> }) => {
    let interval = FAST_INTERVAL;
    while (true) {
      const count = await queue.prefetchJobs();
      if (count > 0) {
        interval = FAST_INTERVAL;
      } else {
        interval = Math.min(interval * 2, MAX_IDLE_INTERVAL);
      }
      await new Promise(resolve => setTimeout(resolve, interval));
    }
  };

  try {
    await Promise.all([
      prefetchLoop(crawlFinishedQueue),
      (async () => {
        let interval = FAST_INTERVAL;
        while (true) {
          if (config.NUQ_PREFETCH_WORKER_HEARTBEAT_URL) {
            fetch(config.NUQ_PREFETCH_WORKER_HEARTBEAT_URL).catch(() => {});
          }
          const count = await scrapeQueue.prefetchJobs();
          if (count > 0) {
            interval = FAST_INTERVAL;
          } else {
            interval = Math.min(interval * 2, MAX_IDLE_INTERVAL);
          }
          await new Promise(resolve => setTimeout(resolve, interval));
        }
      })(),
    ]);
  } catch (error) {
    logger.error("Error in prefetch worker", { error });
    process.exit(1);
  }

  logger.info("All prefetch workers exited. Shutting down...");
  await shutdown();
})();
