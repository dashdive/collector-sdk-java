import fs from "fs";
import { promisify } from "util";
import path from "path";

type ClientAction = {
  request: {
    method: "GET" | "POST" | "PUT";
    path: string;
  };
  headers: Record<string, string>;
  body: object;
};

async function loadClientTrafficFile(): Promise<ClientAction[]> {
  const clientTrafficFile = path.join(__dirname, "test_output.json");
  const readFileAsync = promisify(fs.readFile);
  const clientTrafficString = await readFileAsync(clientTrafficFile, "utf8");
  const clientTraffic = JSON.parse(clientTrafficString);

  if (!Array.isArray(clientTraffic)) {
    throw new Error("Expected client traffic to be an array");
  }
  if (
    clientTraffic.some(
      (item) =>
        !("request" in item) || !("headers" in item) || !("body" in item)
    )
  ) {
    throw new Error("Expected client traffic to be an array of action objects");
  }
  return clientTraffic;
}

let clientTraffic: ClientAction[];
describe("Confirm test client traffic", () => {
  beforeAll(async () => {
    clientTraffic = await loadClientTrafficFile();
  });

  test("Dummy test", async () => {
    console.log("Client traffic:", clientTraffic.length);
    expect(clientTraffic.length).toBeGreaterThanOrEqual(1);
  });
});
