import express from "express";
import bodyParser from "body-parser";
import { ArgumentParser } from "argparse";
import fs from "fs";
import path from "path";

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

const parser = new ArgumentParser({
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
    route: {
      method: req.method,
      path: req.path,
    },
    headers: req.headers,
    body: req.body,
  };
}

function formatJson(json: any): string {
  return JSON.stringify(json, null, 4);
}

function writeRequestToFile(request: object) {
  const absoluteFilePath = path.resolve(filePath);
  fs.mkdirSync(path.dirname(absoluteFilePath), { recursive: true });
  fs.appendFileSync(absoluteFilePath, formatJson(request) + ",\n");
}

fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
fs.writeFileSync(path.resolve(filePath), "[\n");

app.get("/s3/recommendedBatchSize", (req, res) => {
  const requestObject = getOutputRequestObject(req);
  writeRequestToFile(requestObject);
  res.send("100");
});

app.all("*", (req, res) => {
  const requestObject = getOutputRequestObject(req);
  writeRequestToFile(requestObject);
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

process.on("SIGTERM", () => {
  console.log("SIGTERM signal received: closing HTTP server");

  // Close the JSON array in the file
  const absoluteFilePath = path.resolve(filePath);
  fs.appendFileSync(absoluteFilePath, "]");

  server.close(() => {
    console.log("HTTP server closed");
    process.exit(0);
  });
});
