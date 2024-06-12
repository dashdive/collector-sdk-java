import express from "express";
import bodyParser from "body-parser";
import argparse from "argparse";
import fs from "fs";

const app = express();
const defaultPort = 3232;

function validPort(port: string): number {
  const portNumber = parseInt(port, 10);
  if (isNaN(portNumber)) {
    throw new Error(`Invalid port number: expected a number.`);
  }

  const portMin = 2000;
  const portMax = 59999;
  if (portNumber < portMin || portNumber > portMax) {
    throw new Error(
      `Invalid port number: expected a number between ${portMin} and ${portMax}, inclusive.`
    );
  }

  return portNumber;
}

const parser = new argparse.ArgumentParser({
  description: "Mock usage collector server.",
});
parser.add_argument("-p", "--port", {
  help: "Port number to listen on.",
  type: validPort,
  required: false,
  default: defaultPort,
});
parser.add_argument("-f", "--file", {
  help: "File to which to write request logs.",
  required: true,
});
const args = parser.parse_args();
const port = args.port;
const filePath = args.file;

// NOTE: `Content-Type` header must be set for body parser to parse the body
app.use(bodyParser.json({ strict: false }));

function getOutputRequestObject(req: express.Request): object {
  return {
    request: {
      method: req.method,
      path: req.path,
    },
    headers: req.headers,
    body: req.body,
  };
}

const requestObjects = [];
app.get("/s3/recommendedBatchSize", (req, res) => {
  requestObjects.push(getOutputRequestObject(req));
  res.send("100");
});

app.all("*", (req, res) => {
  requestObjects.push(getOutputRequestObject(req));
  res.sendStatus(200);
});

const server = app.listen(port, () => {
  console.log(`Mock usage collector listening on port: ${port}.`);
  console.log(`Send an example:`);
  console.log(`  curl -X POST 127.0.0.1:${port}/example_route \\
    -H "Content-Type: application/json" \\
    -d '{ "key": "value" }'`);
  console.log("\n");
});

function formatJson(json: any): string {
  return JSON.stringify(json, null, 4);
}

process.on("SIGTERM", () => {
  console.log("SIGTERM signal received: writing logs and closing HTTP server");

  const content = formatJson(requestObjects);
  fs.writeFileSync(filePath, content);

  server.close(() => {
    console.log("HTTP server closed");
    process.exit(0);
  });
});
