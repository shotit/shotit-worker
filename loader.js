import "dotenv/config.js";
import WebSocket from "ws";
import xmldoc from "xmldoc";
import lzma from "lzma-native";
import fetch from "node-fetch";
import { MilvusClient } from "@zilliz/milvus2-sdk-node";

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
          dim: "360",
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

const getCharCodesVector = (str, length = 360, base = 100000) => {
  let charCodeArr = Array(length).fill(0);

  // str.length should be less than parameter length
  for (let i = 0; i < str.length; i++) {
    let code = str.charCodeAt(i);
    charCodeArr[i] = parseFloat(code / base);
  }

  return charCodeArr;
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
          cl_ha: getCharCodesVector(doc.cl_ha.split(" ").join(""), 360, 100000),
          primary_key: getPrimaryKey(doc.cl_hi),
        };
      });

      const milvusClient = new MilvusClient(MILVUS_URL);

      console.log(`Uploading JSON data to Milvus`);

      let startTime = performance.now();
      console.log("Insert begins", startTime);
      // Insert at a batch of 10 thousand each time, if more than that
      let loopCount = jsonData.length / 10000;
      if (loopCount <= 1) {
        await milvusClient.dataManager.insert({
          collection_name: "trace_moe",
          fields_data: jsonData,
        });
      } else {
        for (let i = 0; i < Math.ceil(loopCount); i++) {
          if (i === Math.ceil(loopCount) - 1) {
            await milvusClient.dataManager.insert({
              collection_name: "trace_moe",
              fields_data: jsonData.slice(i * 10000),
            });
            break;
          }
          await milvusClient.dataManager.insert({
            collection_name: "trace_moe",
            fields_data: jsonData.slice(i * 10000, i * 10000 + 10000),
          });
          // Pause 1000ms to prevent GRPC "Error: 14 UNAVAILABLE: Connection dropped"
          // Reference: https://groups.google.com/g/grpc-io/c/xTJ8pUe9F_E
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      // // Parallel, don't use for exceed TPC concurrency limit
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

      startTime = performance.now();
      console.log("Flush begins", startTime);
      await milvusClient.dataManager.flushSync({ collection_names: ["trace_moe"] });
      console.log("Flush done", performance.now() - startTime);

      const index_params = {
        metric_type: "L2",
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
    } catch (error) {
      console.log(error);
      console.log("Reconnecting in 30 seconds");
      await new Promise((resolve) => setTimeout(resolve, 30000));
      fallBack();
    }
  };

  await fallBack();

  await fetch(`${TRACE_API_URL}/loaded/${anilistID}/${encodeURIComponent(fileName)}`, {
    headers: { "x-trace-secret": TRACE_API_SECRET },
  });
  ws.send(data);
  console.log(`Loaded ${file}`);
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
};

closeHandle();
