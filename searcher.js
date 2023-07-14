import "dotenv/config.js";
import express from "express";
import rateLimit from "express-rate-limit";
import fs from "fs-extra";
import os from "os";
import path from "path";
import crypto from "crypto";
import bodyParser from "body-parser";
import fetch from "node-fetch";
import { MilvusClient } from "@zilliz/milvus2-sdk-node";
import JBC from "jsbi-calculator";
const { calculator, BigDecimal } = JBC;

const {
  PORT = 19531,
  SEARCHER_URL,
  MILVUS_URL,
  SOLA_SOLR_LIST,
  TRACE_ALGO,
  TRACE_ACCURACY = 1,
} = process.env;

const SOLR_URL = `${SOLA_SOLR_LIST}${TRACE_ALGO}_0`;

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

const search = async (hash) => {
  const milvusClient = new MilvusClient({
    address: MILVUS_URL,
    timeout: 60 * 1000, // 60s
  });

  await milvusClient.loadCollectionSync({
    collection_name: "shotit",
  });

  const searchParams = {
    anns_field: "cl_ha",
    topk: "15",
    metric_type: "IP",
    params: JSON.stringify({ nprobe: 10 }),
  };

  const normalizedCharCodesVector = getNormalizedCharCodesVector(hash);

  console.log("hash: ");

  console.log(hash);

  console.log("normalizedCharCodesVector: ");

  console.log(normalizedCharCodesVector);

  const results = await milvusClient.search({
    collection_name: "shotit",
    expr: "",
    vectors: [normalizedCharCodesVector],
    search_params: searchParams,
    vector_type: 101, // DataType.FloatVector
    output_fields: ["id", "primary_key"],
  });

  return results;
};

const app = express();

app.disable("x-powered-by");
app.set("trust proxy", 1);

app.use((req, res, next) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");
  next();
});

app.use(
  rateLimit({
    max: 100, // limit each IP to 100 requests
    windowMs: 1000, // per second
    delayMs: 0, // disable delaying - full speed until the max limit is reached
  })
);

app.use(
  bodyParser.raw({
    type: ["image/jpeg", "image/png"],
    limit: "10mb",
  })
);

app.get("/", (req, res) => {
  res.send("OK");
});

app.post("/uploadImage", async (req, res) => {
  const image = req.body;

  let md5 = Date.now();
  const imageHash = crypto.createHash("md5");
  imageHash.update(image);
  md5 = imageHash.digest("hex");

  const typeMap = {};
  // 89504e47 would be converted to Number type by prettier, use this trick
  typeMap[`89504e47`] = "png"; // format header feature
  typeMap[`ffd8ffe0`] = "jpg";
  const ext = typeMap[image.toString("hex", 0, 4)] || "jpg";

  const tempFileName = `${md5}.${ext}`;

  const tempPath = path.join(os.tmpdir(), `milvus`);
  fs.ensureDirSync(tempPath);

  fs.writeFileSync(path.join(tempPath, tempFileName), image);
  console.log(`wirte file ${tempFileName}`);
  res.send(`/retriveImage?name=${tempFileName}`);

  // delete tempFile 1 minutes later
  await new Promise((resolve) => setTimeout(resolve, 60 * 1000));
  try {
    fs.unlinkSync(path.join(tempPath, tempFileName));
    console.log(`unlink ${tempFileName}`);
  } catch (error) {
    console.log(error);
  }
});

app.get("/retriveImage", (req, res) => {
  const { name = "" } = req.query;
  const tempPath = path.join(os.tmpdir(), `milvus`);
  fs.ensureDirSync(tempPath);
  const buffer = fs.createReadStream(path.join(tempPath, name));
  const ext = name.split(".")[1] || "jpeg";
  res.setHeader("Content-Type", `image/${ext}`);
  buffer.pipe(res);
});

app.post("/search", async (req, res) => {
  const image = req.body;

  const typeMap = {};
  // 89504e47 would be converted to Number type by prettier, use this trick
  typeMap[`89504e47`] = "png"; // format header feature
  typeMap[`ffd8ffe0`] = "jpeg";
  const ext = typeMap[image.toString("hex", 0, 4)] || "jpeg";

  const imageUrl = await fetch(`${SEARCHER_URL}/uploadImage`, {
    method: "POST",
    body: image,
    headers: { "Content-Type": `image/${ext}` },
  }).then((res) => res.text());

  const fullImageUrl = `${SEARCHER_URL}${imageUrl}`;

  const solrResponse = await fetch(
    `${SOLR_URL}/lireq?extract=${fullImageUrl}&field=cl_ha&ms=false&oh=false&accuracy=${TRACE_ACCURACY}`,
    {
      method: "GET",
      mode: "cors",
    }
  ).then((res) => res.json());

  const { bs_list: hash } = solrResponse;

  const searchResponse = await search(hash.join(" "));

  // console.log(searchResponse);

  const {
    results: docs,
    status: { error_code },
  } = searchResponse;

  // Mimic the solr search response
  const response = {
    RawDocsCount: "0",
    RawDocsSearchTime: "0",
    ReRankSearchTime: "0",
    response: { docs: [] },
  };
  if (error_code === "Success") {
    response["response"]["docs"] = docs;
    res.send(response);
  } else {
    res.status(501).json({
      error: "Internal Server Error",
    });
  }
});

app.listen(PORT, "0.0.0.0", () => console.log(`Server listening on port ${PORT}`));
