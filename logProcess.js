const fs = require("fs");
const zlib = require("zlib");
const readline = require("readline");
const __ = require("highland");

// Connect to the file stream
let lineReader = readline.createInterface({
  input: fs.createReadStream("logs.log.gz").pipe(zlib.createGunzip()),
});

// Generate the file stream
const generator = (push, next) => {
  lineReader.on("line", (line) => {
    push(null, line);
  });
  lineReader.on("close", () => {
    push(null, __.nil);
  });
};

// group by operations and type
const groupByOperationsAndType = (acc, dataArray, operation) => {
  let operationType = dataArray[3].split("operationType:")[1].trim();
  let duration = parseFloat(dataArray[2].split("duration:")[1].trim());
  if (typeof acc.groupByOperation[operation + operationType] === "undefined") {
    acc.groupByOperation[operation + operationType] = {
      min: duration,
      max: duration,
      sum: duration,
      count: 1,
    };
  } else {
    if (duration < acc.groupByOperation[operation + operationType].min) {
      acc.groupByOperation[operation + operationType].min = duration;
    }
    if (duration > acc.groupByOperation[operation + operationType].max) {
      acc.groupByOperation[operation + operationType].max = duration;
    }

    acc.groupByOperation[operation + operationType].sum += duration;
    acc.groupByOperation[operation + operationType].count += 1;
  }

  return acc;
};

// Highland stream processing
__(generator)
  .reduce({}, (acc, data) => {
    if (typeof acc.groupByOperation === "undefined") {
      acc.groupByOperation = {};
    }
    if (typeof acc.operations === "undefined") {
      acc.operations = {};
    }

    if (data.includes("subscription") && !data.includes("opId")) {
      acc.subscriptionCount = (acc.subscriptionCount || 0) + 1;
    } else if (data.includes("query") && !data.includes("opId")) {
      acc.queryCount = (acc.queryCount || 0) + 1;
    } else if (data.includes("mutation") && !data.includes("opId")) {
      acc.mutationCount = (acc.mutationCount || 0) + 1;
    }
    if (data.includes("operation")) {
      const dataArray = data.split("|");
      let operation = dataArray[1].split("operation:")[1].trim();
      if (typeof acc.operations[operation] === "undefined") {
        acc.operations[operation] = 1;
      } else {
        acc.operations[operation] += 1;
      }

      if (data.includes("operationType")) {
        acc = groupByOperationsAndType(acc, dataArray, operation);
      }
    }

    acc.count = (acc.count || 0) + 1;
    return acc;
  })
  .toCallback((err, data) => {
    if (err) {
      return err;
    }
    console.log(
      "How many queries, mutations and subscriptions have been performed?"
    );
    console.log(
      `queries : ${data.queryCount}, mutations : ${data.queryCount} , subscriptions: ${data.queryCount} `
    );

    console.log("What are the counts for the different operations?");
    for (const [key, value] of Object.entries(data.operations)) {
      console.log(`operation: ${key}, count: ${value}`);
    }

    console.log(
      "What are the average duration times grouped by operation type, operation?"
    );
    console.log(
      "What are the max duration times grouped by operation type, operation?"
    );
    console.log(
      "What are the min duration times grouped by operation type, operation?"
    );
    for (const [key, value] of Object.entries(data.groupByOperation)) {
      console.log(
        `grouped by operation type, operation: ${key}, average:${
          value.sum / value.count
        }, max: ${value.max}, max: ${value.min}`
      );
    }
  });
