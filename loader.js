import "dotenv/config.js";
import WebSocket from "ws";
import xmldoc from "xmldoc";
import lzma from "lzma-native";
import fetch from "node-fetch";
import { MilvusClient, DataType, MetricType, IndexType } from "@zilliz/milvus2-sdk-node";
import cron from "node-cron";
import { Worker, isMainThread, workerData, parentPort } from "node:worker_threads";
import { fileURLToPath } from "url";
import lodash from "lodash";
const { chunk, flatten } = lodash;
import JBC from "jsbi-calculator";
const { calculator, BigDecimal } = JBC;

const { TRACE_API_URL, TRACE_API_SECRET, MILVUS_URL } = process.env;

const __filename = fileURLToPath(import.meta.url);

let ws;
const openHandle = async () => {
  console.log("connected");
  ws.send("");
  await initializeMilvusCollection().catch((e) => console.log(e));
};

const initializeMilvusCollection = async () => {
  const milvusClient = new MilvusClient({
    address: MILVUS_URL,
    timeout: 5 * 60 * 1000, // 5 mins
  });

  const params = {
    collection_name: "shotit",
    description: "Shotit Index Data Collection",
    fields: [
      {
        name: "cl_ha",
        description: "Dynamic fields for LIRE Solr",
        data_type: DataType.FloatVector,
        dim: 100,
      },
      // {
      //   name: "cl_hi",
      //   data_type: 21, //DataType.VarChar
      //   max_length: 200,
      //   description: "Metric Spaces Indexing",
      // },
      {
        name: "hash_id",
        data_type: DataType.VarChar,
        max_length: 500,
        description: "${imdbID}/${fileName}/${time}",
      },
      {
        name: "primary_key",
        data_type: DataType.Int64,
        is_primary_key: true,
        description: "Primary Key",
      },
    ],
  };

  const fallBack = async () => {
    try {
      await milvusClient.releaseCollection({ collection_name: "shotit" });
      await milvusClient.createCollection(params);
      console.log('collection_name: "shotit" ensured');
      milvusClient.closeConnection();
    } catch (error) {
      console.log(error);
      console.log("initializeMilvusCollection reconnecting in 3 seconds");
      await new Promise((resolve) => setTimeout(resolve, 1000 * 3));
      await fallBack();
    }
  };

  await fallBack();
};

/**
 * getNormalizedCharCodesVector
 * @param {String} str
 * eg. '3ef d3c 2cc 7b6 9dd 2b6 549 852 582 dfd c5e c01 6af ccf 46f
 *      1a5 5b 4a6 f8b 6d2 6a9 48d 2a1 59d ed5 b78 ac3 75 44d c15
 *      cb3 954 1d9 44f 3a3 15b 44d 331 603 43d fb ef1 4e7 46 e92
 *      ec6 848 c7c 8e8 8df 441 39a aa 6d6 911 9f9 d6f c2c 942 3b3
 *      5b2 94c 521 a4c 6ac b38 7a9 584 d2a 5e3 c30 da1 733 12c fc3
 *      dbd 152 3fa 15a b81 c24 cb beb e21 357 a0e 48e 300 19 827
 *      2c6 b67 651 dba 9a4 b4b 85 d75 f78 c30'
 * @param {Number} length
 * @param {Number} base
 * @returns []Number
 */
const getNormalizedCharCodesVector = (str, length = 100, base = 1) => {
  const arr = str.split(" ").map((el) => parseInt(el, 16));
  let charCodeArr = Array(length).fill(0);

  // arr.length should be less than parameter length
  for (let i = 0; i < arr.length; i++) {
    let code = arr[i];
    charCodeArr[i] = parseFloat(code / base);
  }

  const norm = BigDecimal.sqrt(
    String(
      charCodeArr.reduce((acc, cur) => {
        return acc + cur * cur;
      }, 0)
    )
  ).toString();

  return charCodeArr.map((el) => parseFloat(calculator(`${el} / ${norm}`)));
};

const getPrimaryKey = (str) => {
  let charCodeArr = [];

  // str.length should be less than parameter length
  for (let i = 0; i < str.length; i++) {
    let code = str.charCodeAt(i);
    charCodeArr.push(code);
  }

  return charCodeArr.reduce((acc, cur) => {
    return acc + cur;
  }, 0);
};

/**
 * If isMainThread, spawn a new worker to load hash vectors, and when the
 * loading is done, terminate the worker.
 * @param {*} data
 * @returns
 */
const messageHandle = async (data) => {
  if (isMainThread) {
    // workerData is utilized at the end of this file
    let worker = new Worker(__filename, { workerData: data.toString() });
    console.log("Spawn new Worker: ", worker.threadId);
    const resolve = (payload) => {
      ws.send(payload);
      worker.terminate();
      worker = null;
    };
    worker.on("message", resolve);
  } else {
    const { file } = JSON.parse(data);

    console.log(`Downloading ${file}.xml.xz`);
    const [imdbID, fileName] = file.split("/");
    const res = await fetch(
      `${TRACE_API_URL}/hash/${imdbID}/${encodeURIComponent(fileName)}.xml.xz`,
      {
        headers: { "x-trace-secret": TRACE_API_SECRET },
      }
    );
    if (res.status >= 400) {
      console.log(`Error: Failed to download "${await res.text()}"`);
      ws.send(data);
      return;
    }

    console.log("Unzipping hash");
    const xmlData = await lzma.decompress(Buffer.from(await res.arrayBuffer()));

    console.log("Parsing xml");
    const hashList = new xmldoc.XmlDocument(xmlData).children
      .filter((child) => child.name === "doc")
      .map((doc) => {
        const fields = doc.children.filter((child) => child.name === "field");
        return {
          time: parseFloat(fields.filter((field) => field.attr.name === "id")[0].val),
          cl_hi: fields.filter((field) => field.attr.name === "cl_hi")[0].val,
          cl_ha: fields.filter((field) => field.attr.name === "cl_ha")[0].val,
        };
      })
      .sort((a, b) => a.time - b.time);

    const dedupedHashList = [];
    hashList.forEach((currentFrame) => {
      if (
        !dedupedHashList
          .slice(-24) // get last 24 frames
          .filter((frame) => currentFrame.time - frame.time < 2) // select only frames within 2 seconds
          .some((frame) => frame.cl_hi === currentFrame.cl_hi) // check for exact match frames
      ) {
        dedupedHashList.push(currentFrame);
      }
    });

    const milvusClient = new MilvusClient({
      address: MILVUS_URL,
      timeout: 5 * 60 * 1000, // 5 mins
    });
    // The retry mechanism to prevent GRPC error
    const fallBack = async () => {
      try {
        console.log(`Polish JSON data`);

        // let jsonData = new Array(dedupedHashList.length).fill(null);
        // for (let i = 0; i < dedupedHashList.length; i++) {
        //   const doc = dedupedHashList[i];
        //   jsonData[i] = {
        //     hash_id: `${file}/${doc.time.toFixed(2)}`,
        //     // cl_hi: doc.cl_hi, // reduce index size
        //     cl_ha: getNormalizedCharCodesVector(doc.cl_ha, 100, 1),
        //     primary_key: getPrimaryKey(doc.cl_hi),
        //   };
        // }

        // Parallel operation with 1000 as one unit
        let chunkedJsonData = chunk(new Array(dedupedHashList.length).fill(null), 1000);
        let chunkedDedupedHashList = chunk(dedupedHashList, 1000); // [1,...,2000] => [[1,...,1000],[1001,...,2000]]
        const modifier = (dedupedHashList, jsonData) => {
          for (let i = 0; i < dedupedHashList.length; i++) {
            const doc = dedupedHashList[i];
            jsonData[i] = {
              hash_id: `${file}/${doc.time.toFixed(2)}`,
              // cl_hi: doc.cl_hi, // reduce index size
              cl_ha: getNormalizedCharCodesVector(doc.cl_ha, 100, 1),
              primary_key: getPrimaryKey(doc.cl_hi),
            };
          }
          return jsonData;
        };
        const segments = await Promise.all(
          chunkedDedupedHashList.map((each, index) => {
            return modifier(each, chunkedJsonData[index]);
          })
        );
        const jsonData = flatten(segments);

        // Pause for 1 second to make node arrange the compute resource.
        // Note: not 5 in case of gRPC Error: 13 INTERNAL: No message received
        console.log("Pause for 1 second");
        await new Promise((resolve) => setTimeout(resolve, 1000));

        console.log(`Uploading JSON data to Milvus`);

        let startTime = performance.now();
        console.log("Insert begins", startTime);
        // Insert at a batch of 2 thousand each time, if more than that
        let loopCount = jsonData.length / 2000;
        if (loopCount <= 1) {
          await milvusClient.insert({
            collection_name: "shotit",
            fields_data: jsonData,
          });
        } else {
          for (let i = 0; i < Math.ceil(loopCount); i++) {
            if (i === Math.ceil(loopCount) - 1) {
              await milvusClient.insert({
                collection_name: "shotit",
                fields_data: jsonData.slice(i * 2000),
              });
              break;
            }
            await milvusClient.insert({
              collection_name: "shotit",
              fields_data: jsonData.slice(i * 2000, i * 2000 + 2000),
            });
            // Pause 500ms to prevent GRPC "Error: 14 UNAVAILABLE: Connection dropped"
            // Reference: https://groups.google.com/g/grpc-io/c/xTJ8pUe9F_E
            await new Promise((resolve) => setTimeout(resolve, 500));
          }
        }

        // // Parallel, don't use for exceed TCP concurrency limit
        // // and the following "Error: 14 UNAVAILABLE: Connection dropped"

        // let loopCount = jsonData.length / 10000;
        // if (loopCount <= 1) {
        //   await milvusClient.insert({
        //     collection_name: "shotit",
        //     fields_data: jsonData,
        //   });
        // } else {
        //   const batchList = [];
        //   for (let i = 0; i < Math.ceil(loopCount); i++) {
        //     if (i === Math.ceil(loopCount) - 1) {
        //       batchList.push(jsonData.slice(i * 10000));
        //       break;
        //     }
        //     batchList.push(jsonData.slice(i * 10000, i * 10000 + 10000));
        //   }
        //   await Promise.all(
        //     batchList.map(async (batch) => {
        //       await milvusClient.insert({
        //         collection_name: "shotit",
        //         fields_data: batch,
        //       });
        //     })
        //   );
        // }

        console.log("Insert done", performance.now() - startTime);

        startTime = performance.now();
        console.log("Flush begins", startTime);
        await milvusClient.flushSync({ collection_names: ["shotit"] });
        console.log("Flush done", performance.now() - startTime);

        startTime = performance.now();
        console.log("Index begins", startTime);
        await milvusClient.createIndex({
          collection_name: "shotit",
          field_name: "cl_ha",
          metric_type: MetricType.IP,
          index_type: IndexType.IVF_SQ8,
          params: { nlist: 128 },
        });
        console.log("Index done", performance.now() - startTime);

        /* 
        Not trigger load yet at the index period.
        Take it at the search period to enchence index performance
      */

        // startTime = performance.now();
        // console.log("Load begins", startTime);
        // // Sync trick to prevent gRPC overload so that the follwing large-volume insert
        // // operation would not cause "Error: 14 UNAVAILABLE: Connection dropped"
        // await milvusClient.loadCollectionSync({
        //   collection_name: "shotit",
        // });
        // console.log("Load done", performance.now() - startTime);

        await fetch(`${TRACE_API_URL}/loaded/${imdbID}/${encodeURIComponent(fileName)}`, {
          headers: { "x-trace-secret": TRACE_API_SECRET },
        });
        // ws.send(data);
        console.log(`Loaded ${file}`);
        milvusClient.closeConnection();
        parentPort.postMessage(data);
      } catch (error) {
        console.log(error);
        console.log("Reconnecting in 60 seconds");
        await new Promise((resolve) => setTimeout(resolve, 60000));
        await fallBack();
      }
    };

    await fallBack();
  }
};

const closeHandle = async () => {
  console.log(`Connecting to ${TRACE_API_URL.replace(/^http/, "ws")}/ws`);
  ws = new WebSocket(`${TRACE_API_URL.replace(/^http/, "ws")}/ws`, {
    headers: { "x-trace-secret": TRACE_API_SECRET, "x-trace-worker-type": "load" },
  });
  ws.on("open", openHandle);
  ws.on("message", messageHandle);
  ws.on("error", async (e) => {
    console.log(e);
  });
  ws.on("close", async (e) => {
    console.log(`WebSocket closed (Code: ${e})`);
    console.log("Reconnecting in 5 seconds");
    await new Promise((resolve) => setTimeout(resolve, 5000));
    closeHandle();
  });
  // Flush once a day at 00:00 am.
  cron.schedule("0 0 * * *", async () => {
    startTime = performance.now();
    const milvusClient = new MilvusClient({
      address: MILVUS_URL,
      timeout: 5 * 60 * 1000, // 5 mins
    });
    console.log("Flush begins", startTime);
    await milvusClient.flushSync({ collection_names: ["shotit"] });
    console.log("Flush done", performance.now() - startTime);
    milvusClient.closeConnection();
  });
};

if (!isMainThread) {
  messageHandle(workerData);
} else {
  closeHandle();
}
