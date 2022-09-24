import "dotenv/config.js";
import WebSocket from "ws";
import xmldoc from "xmldoc";
import lzma from "lzma-native";
import fetch from "node-fetch";
import { MilvusClient } from "@zilliz/milvus2-sdk-node";
import cron from "node-cron";
import JBC from "jsbi-calculator";
const { calculator } = JBC;

const { TRACE_API_URL, TRACE_API_SECRET, MILVUS_URL } = process.env;

let ws;
const openHandle = async () => {
  console.log("connected");
  ws.send("");
  await initializeMilvusCollection().catch((e) => console.log(e));
};

const initializeMilvusCollection = async () => {
  const milvusClient = new MilvusClient(MILVUS_URL);

  const params = {
    collection_name: "trace_moe",
    description: "Trace.moe Index Data Collection",
    fields: [
      {
        name: "cl_ha",
        description: "Dynamic fields for LIRE Solr",
        data_type: 101, // DataType.FloatVector
        type_params: {
          dim: "100",
        },
      },
      // {
      //   name: "cl_hi",
      //   data_type: 21, //DataType.VARCHAR
      //   type_params: {
      //     max_length: "200",
      //   },
      //   description: "Metric Spaces Indexing",
      // },
      {
        name: "id",
        data_type: 21, //DataType.VARCHAR
        type_params: {
          max_length: "500",
        },
        description: "${anilistID}/${fileName}/${time}",
      },
      {
        name: "primary_key",
        data_type: 5, //DataType.Int64
        is_primary_key: true,
        description: "Primary Key",
      },
    ],
  };

  await milvusClient.collectionManager.releaseCollection({ collection_name: "trace_moe" });

  await milvusClient.collectionManager.createCollection(params);
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

  const norm = Math.sqrt(
    charCodeArr.reduce((acc, cur) => {
      return acc + cur * cur;
    }, 0)
  );

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

const messageHandle = async (data) => {
  const { file } = JSON.parse(data.toString());

  console.log(`Downloading ${file}.xml.xz`);
  const [anilistID, fileName] = file.split("/");
  const res = await fetch(
    `${TRACE_API_URL}/hash/${anilistID}/${encodeURIComponent(fileName)}.xml.xz`,
    {
      headers: { "x-trace-secret": TRACE_API_SECRET },
    }
  );
  if (res.status >= 400) {
    console.log(`Error: Fail to download "${await res.text()}"`);
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

  // The retry mechanism to prevent GRPC error
  const fallBack = async () => {
    try {
      const jsonData = dedupedHashList.map((doc) => {
        return {
          id: `${file}/${doc.time.toFixed(2)}`,
          // cl_hi: doc.cl_hi, // reduce index size
          cl_ha: getNormalizedCharCodesVector(doc.cl_ha, 100, 1),
          primary_key: getPrimaryKey(doc.cl_hi),
        };
      });

      const milvusClient = new MilvusClient(MILVUS_URL);

      console.log(`Uploading JSON data to Milvus`);

      let startTime = performance.now();
      console.log("Insert begins", startTime);
      // Insert at a batch of 1 thousand each time, if more than that
      let loopCount = jsonData.length / 1000;
      for (let i = 0; i < Math.ceil(loopCount); i++) {
        await milvusClient.dataManager.insert({
          collection_name: "trace_moe",
          fields_data: jsonData.slice(i * 1000, i * 1000 + 1000), // slice still works when less than 1000
        });

        // Pause 500ms to prevent GRPC "Error: 14 UNAVAILABLE: Connection dropped"
        // Reference: https://groups.google.com/g/grpc-io/c/xTJ8pUe9F_E
        await new Promise((resolve) => setTimeout(resolve, 500));
      }

      // // Parallel, don't use for exceed TCP concurrency limit
      // // and the following "Error: 14 UNAVAILABLE: Connection dropped"

      // let loopCount = jsonData.length / 10000;
      // if (loopCount <= 1) {
      //   await milvusClient.dataManager.insert({
      //     collection_name: "trace_moe",
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
      //       await milvusClient.dataManager.insert({
      //         collection_name: "trace_moe",
      //         fields_data: batch,
      //       });
      //     })
      //   );
      // }

      console.log("Insert done", performance.now() - startTime);

      // Turn to use cron schedule for flush, once a day at 00:00 am.
      // startTime = performance.now();
      // console.log("Flush begins", startTime);
      // await milvusClient.dataManager.flushSync({ collection_names: ["trace_moe"] });
      // console.log("Flush done", performance.now() - startTime);

      const index_params = {
        metric_type: "IP",
        index_type: "IVF_SQ8",
        params: JSON.stringify({ nlist: 128 }),
      };

      startTime = performance.now();
      console.log("Index begins", startTime);
      await milvusClient.indexManager.createIndex({
        collection_name: "trace_moe",
        field_name: "cl_ha",
        extra_params: index_params,
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
      // await milvusClient.collectionManager.loadCollectionSync({
      //   collection_name: "trace_moe",
      // });
      // console.log("Load done", performance.now() - startTime);

      await fetch(`${TRACE_API_URL}/loaded/${anilistID}/${encodeURIComponent(fileName)}`, {
        headers: { "x-trace-secret": TRACE_API_SECRET },
      });
      ws.send(data);
      console.log(`Loaded ${file}`);
    } catch (error) {
      console.log(error);
      console.log("Reconnecting in 60 seconds");
      await new Promise((resolve) => setTimeout(resolve, 60000));
      await fallBack();
    }
  };

  await fallBack();
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
    const milvusClient = new MilvusClient(MILVUS_URL);
    console.log("Flush begins", startTime);
    await milvusClient.dataManager.flushSync({ collection_names: ["trace_moe"] });
    console.log("Flush done", performance.now() - startTime);
  });
};

closeHandle();
