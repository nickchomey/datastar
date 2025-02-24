import { Hono } from "jsr:@hono/hono";
import { ServerSentEventGenerator } from "./serverSentEventGenerator.ts";
import type { Jsonifiable } from "npm:type-fest";

const app = new Hono();

app.get("/", (c) => {
  return c.html(
    `<html>
       <head>
         <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0-beta.6/bundles/datastar.js"></script>
       </head>
       <body>
         <div id="toMerge" data-signals-foo="'World'" data-on-load="@get('/merge')">Hello</div>
       </body>
     </html>`
  );
});

app.get("/test", async (c) => {
  const reader = await ServerSentEventGenerator.readSignals(c.req);
  if (reader.success === true) {
    const events = reader.signals.events;
    if (isEventArray(events)) {
      return ServerSentEventGenerator.stream(c, (stream) => {
        testEvents(stream, events);
        stream.close();
      });
    }
  }
  return c.text("Invalid or missing signals", 400);
});

app.get("/await", async (c) => {
  return ServerSentEventGenerator.stream(c, async (stream) => {
    stream.mergeFragments('<div id="toMerge">Merged</div>');
    await delay(5000);
    stream.mergeFragments('<div id="toMerge">After 5 seconds</div>');
    stream.close();
  });
});

function delay(milliseconds: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function isEventArray(
  events: unknown,
): events is (Record<string, Jsonifiable> & { type: string })[] {
  return Array.isArray(events) && events.every((event) => {
    return typeof event === "object" && event !== null &&
      typeof (event as any).type === "string";
  });
}

function testEvents(
  stream: ServerSentEventGenerator,
  events: Record<string, Jsonifiable>[],
) {
  events.forEach((event) => {
    const { type, ...e } = event;
    switch (type) {
      case "mergeFragments":
        if (e && typeof e === "object" && "fragments" in e) {
          const { fragments, ...options } = e;
          stream.mergeFragments(fragments as string, options || undefined);
        }
        break;
      case "removeFragments":
        if (e && typeof e === "object" && "selector" in e) {
          const { selector, ...options } = e;
          stream.removeFragments(selector as string, options || undefined);
        }
        break;
      case "mergeSignals":
        if (e && typeof e === "object" && "signals" in e) {
          const { signals, ...options } = e;
          stream.mergeSignals(
            signals as Record<string, Jsonifiable>,
            options || undefined,
          );
        }
        break;
      case "removeSignals":
        if (e && typeof e === "object" && "paths" in e) {
          const { paths, ...options } = e;
          stream.removeSignals(paths as string[], options || undefined);
        }
        break;
      case "executeScript":
        if (e && typeof e === "object" && "script" in e) {
          const { script, ...options } = e;
          stream.executeScript(script as string, options || undefined);
        }
        break;
    }
  });
}

Deno.serve({ port: 8000 }, app.fetch);
