import fs from "fs";
import { promisify } from "util";
import path from "path";
import { ArgumentParser } from "argparse";
import lodash from "lodash";

type MethodType = "GET" | "POST";
type Route = {
  method: MethodType;
  path: string;
};

type ClientRequest = {
  route: Route;
  headers: Record<string, string>;
  body: object;
};

type SerializedRequest = string;
type RequestsByRoute = Record<SerializedRequest, ClientRequest[]>;

async function loadClientRequestsFile(): Promise<ClientRequest[]> {
  const clientRequestsFile = path.join(__dirname, "test_output.json");
  const readFileAsync = promisify(fs.readFile);
  const clientRequestsString = await readFileAsync(clientRequestsFile, "utf8");
  const clientRequests = JSON.parse(clientRequestsString);

  if (!Array.isArray(clientRequests)) {
    throw new Error("Expected client requests to be an array");
  }
  if (
    clientRequests.some(
      (item) => !("route" in item) || !("headers" in item) || !("body" in item)
    )
  ) {
    throw new Error(
      "Expected client requests to be an array of action objects"
    );
  }
  return clientRequests;
}

function serializeRoute(route: Route): SerializedRequest {
  return `${route.method}:${route.path}`;
}

function parseClientRequests(clientRequests: ClientRequest[]): RequestsByRoute {
  const requestsByRoute: RequestsByRoute = {};
  for (const action of clientRequests) {
    const serializedRequest = serializeRoute(action.route);
    if (!(serializedRequest in requestsByRoute)) {
      requestsByRoute[serializedRequest] = [];
    }
    requestsByRoute[serializedRequest].push(action);
  }
  return requestsByRoute;
}

// Example: `pnpm test -- --sdk-version=1.0.0`
const parser = new ArgumentParser({
  description: "Jest test for client requests.",
});
parser.add_argument("-v", "--sdk-version", {
  help: "Expected SDK version.",
  required: true,
});
parser.add_argument("-t", "--run-type", {
  help: "Type of test that was run (either 'cloud' or 'local').",
  required: true,
  choices: ["cloud", "local"],
});
const args = parser.parse_args();
const sdkVersion = args.sdk_version;
type RunType = "cloud" | "local";
const runType: RunType = args.run_type;

function expectArraysEqualIgnoringOrder<T>(actual: T[], expected: T[]): void {
  expect(actual.sort()).toEqual(expected.sort());
}

function combineAllItems<I, O>(
  inputArray: I[],
  itemListKey: string,
  predicate: (inputElement: I) => boolean
): O[] {
  const filteredArray = inputArray.filter(predicate);
  return lodash.flatMap(filteredArray, (item) => lodash.get(item, itemListKey));
}

const orderArray = [
  "HeadBucket",
  "CreateBucket",
  "PutObject:java_payload1.txt",
  "PutObject:java_payload2.txt",
  "GetObject:java_payload1.txt",
  "GetObject:java_payload2.txt",
  "DeleteObject:java_payload1.txt",
  "DeleteObject:java_payload2.txt",
  "CreateMultipartUpload",
  "UploadPart:1",
  "UploadPart:2",
  "CompleteMultipartUpload",
  "GetObject:multipart.txt",
  "DeleteObject:multipart.txt",
  "DeleteBucket",
];
const orderMap = new Map<string, number>(
  orderArray.map((action, index) => [action, index])
);
function serializeEventForOrdering(event: object): string {
  const action = event["action"];
  const objectKey = event["objectKey"];
  const partNumber = event["partNumber"];
  if (objectKey) {
    return `${action}:${objectKey}`;
  } else if (partNumber) {
    return `${action}:${partNumber}`;
  } else {
    return action;
  }
}
function getOrderedEventArray(events: object[]): object[] {
  return lodash.sortBy(events, (e) => orderMap[serializeEventForOrdering(e)]);
}

type ExpectedEventsArgs = {
  didHeadBucketSucceed: boolean;
  expectedIsBillableEgress: boolean;
  expectedProvider: string;
  expectedBucketName: string;
  expectedUploadId: string;
  expectedCustomerId: string;
};
function getExpectedOrderedBucketEvents({
  didHeadBucketSucceed,
  expectedIsBillableEgress,
  expectedProvider,
  expectedBucketName,
  expectedUploadId,
  expectedCustomerId,
}: ExpectedEventsArgs): object[] {
  return [
    {
      action: didHeadBucketSucceed ? "HeadBucket" : "CreateBucket",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      customerId: expectedCustomerId,
    },
    {
      action: "PutObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      objectKey: "java_payload1.txt",
      versionId: "",
      bytes: 3,
      customerId: expectedCustomerId,
    },
    {
      action: "PutObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      objectKey: "java_payload2.txt",
      versionId: "",
      bytes: 4,
      customerId: expectedCustomerId,
    },
    {
      action: "GetObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      versionId: "",
      objectKey: "java_payload1.txt",
      bytes: 3,
      isBillableEgress: expectedIsBillableEgress,
      customerId: expectedCustomerId,
    },
    {
      action: "GetObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      versionId: "",
      objectKey: "java_payload2.txt",
      bytes: 4,
      isBillableEgress: expectedIsBillableEgress,
      customerId: expectedCustomerId,
    },
    {
      action: "DeleteObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      objectKey: "java_payload1.txt",
      resultingDeleteMarkerVersionId: "",
      hardDeletedVersionId: "",
      customerId: expectedCustomerId,
    },
    {
      action: "DeleteObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      objectKey: "java_payload2.txt",
      resultingDeleteMarkerVersionId: "",
      hardDeletedVersionId: "",
      customerId: expectedCustomerId,
    },
    {
      action: "CreateMultipartUpload",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      uploadId: expectedUploadId,
      objectKey: "multipart.txt",
      customerId: expectedCustomerId,
    },
    {
      action: "UploadPart",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      uploadId: expectedUploadId,
      partNumber: 1,
      bytes: 5697000,
      customerId: expectedCustomerId,
    },
    {
      action: "UploadPart",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      uploadId: expectedUploadId,
      partNumber: 2,
      bytes: 3,
      customerId: expectedCustomerId,
    },
    {
      action: "CompleteMultipartUpload",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      uploadId: expectedUploadId,
      objectKey: "multipart.txt",
      versionId: "",
      customerId: expectedCustomerId,
    },
    {
      action: "GetObject",
      bucket: expectedBucketName,
      timestamp: expect.any(String),
      provider: expectedProvider,
      versionId: "",
      objectKey: "multipart.txt",
      bytes: 5697003,
      isBillableEgress: expectedIsBillableEgress,
      customerId: expectedCustomerId,
    },
    {
      action: "DeleteObject",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      objectKey: "multipart.txt",
      resultingDeleteMarkerVersionId: "",
      hardDeletedVersionId: "",
      customerId: expectedCustomerId,
    },
    {
      action: "DeleteBucket",
      timestamp: expect.any(String),
      provider: expectedProvider,
      bucket: expectedBucketName,
      customerId: expectedCustomerId,
    },
  ];
}

let allRequests: ClientRequest[] = [];
let requestsByRoute: RequestsByRoute = {};
describe("Confirm test client requests", () => {
  beforeAll(async () => {
    allRequests = await loadClientRequestsFile();
    requestsByRoute = parseClientRequests(allRequests);
  });

  test("Content type header set if and only if applicable", () => {
    const contentTypeHeaderKey = "content-type";
    const contentTypeHeaderValue = "application/json";
    for (const clientRequest of allRequests) {
      const isPost = clientRequest.route.method === "POST";
      const hasBody = Object.keys(clientRequest.body).length > 0;
      const shouldHaveContentType = isPost && hasBody;
      const contentTypeValue = clientRequest.headers[contentTypeHeaderKey];
      if (shouldHaveContentType) {
        expect(contentTypeValue).toBeDefined();
        expect(contentTypeValue).toEqual(contentTypeHeaderValue);
      } else {
        expect(contentTypeValue).toBeUndefined();
      }
    }
  });

  test("API key header always set", () => {
    const apiKeyHeaderKey = "x-api-key";
    const apiKeyHeaderValue =
      "TESTKEYZ.yiXYc+TQZaJvcNq80KA7S6+eAfCPsW4kR59ooVAu3pj/Eqjo";
    for (const clientRequest of allRequests) {
      const apiKeyValue = clientRequest.headers[apiKeyHeaderKey];
      expect(apiKeyValue).toBeDefined();
      expect(apiKeyValue).toEqual(apiKeyHeaderValue);
    }
  });

  test("User agent header always set", () => {
    const userAgentHeaderKey = "user-agent";
    for (const clientRequest of allRequests) {
      const userAgentValue = clientRequest.headers[userAgentHeaderKey];
      expect(userAgentValue).toBeDefined();
      expect(userAgentValue).toContain("Java-http-client");
      expect(userAgentValue).toContain(`v:${sdkVersion}`);
    }
  });

  test("All route/method pairs are expected", () => {
    const expectedRequestTypes = new Set([
      "GET:/s3/recommendedBatchSize",
      "GET:/ping",
      "POST:/telemetry/lifecycle",
      "POST:/s3/batch",
      "POST:/telemetry/extractionIssues",
      "POST:/telemetry/metricsIncremental",
    ]);
    for (const serializedRequest of Object.keys(requestsByRoute)) {
      expect(expectedRequestTypes.has(serializedRequest)).toBeTruthy();
    }
  });

  test("/ping and /recommendedBatchSize called as expected", () => {
    const pingRequests = requestsByRoute["GET:/ping"];
    const recommendedBatchSizeRequests =
      requestsByRoute["GET:/s3/recommendedBatchSize"];

    expect(pingRequests).toHaveLength(1);
    expect(recommendedBatchSizeRequests).toHaveLength(1);

    expect(pingRequests[0].body).toEqual({});
    expect(recommendedBatchSizeRequests[0].body).toEqual({});
  });

  test("Single valid instance ID across all requests", () => {
    const instanceIds = new Set<string>();
    for (const clientRequest of allRequests) {
      if (!clientRequest.route.path.startsWith("/telemetry")) {
        continue;
      }

      const instanceId = clientRequest.body["instanceId"];
      expect(instanceId).toBeDefined();
      instanceIds.add(instanceId);
    }

    expect(instanceIds.size).toEqual(1);
    const singleInstanceId = instanceIds.values().next().value;

    const uuidSizeBytes = 16;
    const bitsPerByte = 8;
    const bitsPerChar = 6;
    const expectedUuidLength = Math.ceil(
      (uuidSizeBytes * bitsPerByte) / bitsPerChar
    );
    expect(singleInstanceId.length).toEqual(expectedUuidLength);
  });

  test("Startup and shutdown events called as expected", () => {
    const lifecycleRequests = requestsByRoute["POST:/telemetry/lifecycle"];

    expect(lifecycleRequests).toHaveLength(2);

    const startupEvent = lifecycleRequests.find(
      (request) => request.body["eventType"] === "startup"
    );
    const shutdownEvent = lifecycleRequests.find(
      (request) => request.body["eventType"] === "shutdown"
    );
    expect(startupEvent).toBeDefined();
    expect(shutdownEvent).toBeDefined();

    const startupEventBody = startupEvent.body;
    const shutdownEventBody = shutdownEvent.body;

    expect(startupEventBody["instanceInfo"]).toBeDefined();
    const generalInfoKeys = [
      "classInstanceId",
      "machinePid",
      "logicalProcessorCount",
      "osDistroInfo",
      "javaVersion",
    ];
    const imdInfoKeys = [
      "imdEc2InstanceId",
      "imdPublicIpv4",
      "imdRegion",
      "imdAvailabilityZone",
      "imdAvailabilityZoneId",
      "imdAmiId",
      "imdInstanceType",
    ];
    expectArraysEqualIgnoringOrder(
      Object.keys(startupEventBody["instanceInfo"]),
      [...generalInfoKeys, ...imdInfoKeys]
    );
    for (const generalInfoKey of generalInfoKeys) {
      expect(startupEventBody["instanceInfo"][generalInfoKey]).toBeDefined();
    }
    for (const imdInfoKey of imdInfoKeys) {
      const actualValue = startupEventBody["instanceInfo"][imdInfoKey];
      if (runType === "cloud") {
        expect(typeof actualValue === "string").toBeTruthy();
        expect(actualValue.length).toBeGreaterThan(0);
      } else {
        expect(actualValue).toBeNull();
      }
    }

    expect(shutdownEventBody["metricsTotal"]).toBeDefined();
    const metricsTotalKeys = [
      "events_enqueued",
      "events_sent",
      "events_dropped_from_queue",
      "events_dropped_send_failure",
      "events_dropped_parse_error",
      "batches_enqueued",
      "batches_sent",
      "batches_dropped_from_queue",
      "batches_dropped_send_failure",
      "batches_dropped_parse_error",
      "events_with_warnings",
      "events_with_errors",
    ];
    expectArraysEqualIgnoringOrder(
      Object.keys(shutdownEventBody["metricsTotal"]),
      metricsTotalKeys
    );
  });

  test("Batched events match all expected events", () => {
    const allEvents: object[] = combineAllItems(
      allRequests,
      "body",
      (r) => serializeRoute(r.route) === "POST:/s3/batch"
    );
    const eventsByBucket = lodash.groupBy(allEvents, (e) => e["bucket"]);
    expect(Object.keys(eventsByBucket).length).toEqual(3);

    const timestampRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
    for (const [bucket, events] of Object.entries(eventsByBucket)) {
      const didHeadBucketSucceed = events.some(
        (e) => e["action"] === "HeadBucket"
      );
      const expectedIsBillableEgress =
        !bucket.includes("sameregion") || args.run_type === "local";
      const expectedProvider = "aws";
      const expectedCustomerId = "dummy-customer";
      const expectedUploadId: string = events.find(
        (e) => e["action"] === "CreateMultipartUpload"
      )?.["uploadId"];

      expect(expectedUploadId).toBeDefined();

      const expectedBucketEvents = getExpectedOrderedBucketEvents({
        didHeadBucketSucceed,
        expectedIsBillableEgress,
        expectedProvider,
        expectedBucketName: bucket,
        expectedUploadId,
        expectedCustomerId,
      });
      expect(getOrderedEventArray(events)).toEqual(expectedBucketEvents);
      for (const event of events) {
        expect(event["timestamp"]).toMatch(timestampRegex);
      }
    }
  });

  test("Extraction issues contain expected errors", () => {
    const allIssues: object[] = combineAllItems(
      allRequests,
      "body.eventsWithIssues",
      (r) => serializeRoute(r.route) === "POST:/telemetry/extractionIssues"
    );

    // If any of the 3 expected buckets don't exist, `HeadBucket` will
    // give an error in addition to the 4 baseline expected errors
    const minLength = 4;
    const maxLength = 7;
    expect(
      minLength <= allIssues.length && allIssues.length <= maxLength
    ).toBeTruthy();
    for (const issue of allIssues) {
      expect(issue).toEqual({
        dataPayload: {},
        hasIrrecoverableErrors: true,
        telemetryWarnings: [],
        telemetryErrors: [expect.any(Object)],
      });
    }

    const allErrors = allIssues.map((i) => i["telemetryErrors"]).flat();
    for (const error of allErrors) {
      expect(error).toEqual({
        type: "ON_EXECUTION_FAILURE",
        domain: null,
        exception: [expect.any(Object)],
      });
    }

    const msg_keyNotFound = "key does not exist";
    const msg_bucketNotFound = "bucket does not exist";

    const allExceptionMessages = allErrors.map((e) => {
      const message: string = e["exception"][0]["exceptionMessage"];
      const firstParen = message.indexOf("(");
      const trimmedMessage = message.slice(0, firstParen - 1);

      if (trimmedMessage.includes(msg_keyNotFound)) {
        return msg_keyNotFound;
      } else if (trimmedMessage.includes(msg_bucketNotFound)) {
        return msg_bucketNotFound;
      } else {
        return trimmedMessage;
      }
    });
    const messageOccurrences = lodash.countBy(allExceptionMessages);
    expect(messageOccurrences[msg_keyNotFound]).toEqual(3);
    expect(messageOccurrences[msg_bucketNotFound]).toEqual(1);

    const uniqueMessageCount = Object.keys(messageOccurrences).length;
    expect([2, 3]).toContain(uniqueMessageCount);
    // This is true if buckets didn't exist (`HeadBucket` gives errors)
    if (uniqueMessageCount === 3) {
      const remainingKey = Object.keys(messageOccurrences).find(
        (key) => key !== msg_keyNotFound && key !== msg_bucketNotFound
      );
      expect(messageOccurrences[remainingKey]).toEqual(3);
    }
  });
});
