/* Amplify Params - DO NOT EDIT
	ENV
	REGION
	CLIENT_ID
	CLIENT_SECRET
	REFRESH_TOKEN
	AUTH_URL
	BASE_URL
	MARKETPLACE_ID
Amplify Params - DO NOT EDIT */

const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');
const AWS = require('aws-sdk');
const express = require('express');
const bodyParser = require('body-parser');
const awsServerlessExpressMiddleware = require('aws-serverless-express/middleware');
const axios = require('axios');
const zlib = require('zlib');

const S3 = new AWS.S3();
const secretsClient = new SecretsManagerClient({ region: 'us-east-1' });

const SECRET_NAME = 'malkagoods/sp-api-secret';
const SECRETS_TTL_MS = 5 * 60 * 1000;
const ACCESS_TOKEN_SAFETY_MARGIN_MS = 60 * 1000;
const REPORT_POLL_INTERVAL_MS = 10_000;
const REPORT_POLL_MAX_ATTEMPTS = 60;
const ASIN_CHUNK_SIZE = 20;

const HTTP_TIMEOUTS_MS = {
  auth: 10_000,
  reports: 10_000,
  reportDownload: 60_000,
  catalog: 30_000,
};

let cachedSecrets = null;
let secretsExpiresAt = 0;
let inflightSecretsPromise = null;
let cachedAccessToken = null;
let accessTokenExpiresAt = 0;
let inflightTokenPromise = null;

async function getSecrets() {
  const now = Date.now();
  if (cachedSecrets && now < secretsExpiresAt) return cachedSecrets;
  if (inflightSecretsPromise) return inflightSecretsPromise;

  inflightSecretsPromise = (async () => {
    try {
      const response = await secretsClient.send(
        new GetSecretValueCommand({
          SecretId: SECRET_NAME,
          VersionStage: 'AWSCURRENT',
        })
      );
      const parsed = response.SecretString
        ? JSON.parse(response.SecretString)
        : {};

      // Env-var fallback lets the cutover to Secrets Manager happen without downtime:
      // keep REFRESH_TOKEN/CLIENT_ID env vars populated until the secret is updated.
      cachedSecrets = {
        SP_CLIENT_SECRET: parsed.SP_CLIENT_SECRET,
        REFRESH_TOKEN: parsed.REFRESH_TOKEN || process.env.REFRESH_TOKEN,
        CLIENT_ID: parsed.CLIENT_ID || process.env.CLIENT_ID,
      };
      secretsExpiresAt = Date.now() + SECRETS_TTL_MS;
      return cachedSecrets;
    } finally {
      inflightSecretsPromise = null;
    }
  })();
  return inflightSecretsPromise;
}

async function getAccessToken() {
  const now = Date.now();
  if (
    cachedAccessToken &&
    now < accessTokenExpiresAt - ACCESS_TOKEN_SAFETY_MARGIN_MS
  ) {
    return cachedAccessToken;
  }
  if (inflightTokenPromise) return inflightTokenPromise;

  inflightTokenPromise = (async () => {
    try {
      console.log('LWA cache miss — minting new access token');
      const secrets = await getSecrets();
      const { data } = await axios.post(
        process.env.AUTH_URL,
        {
          grant_type: 'refresh_token',
          refresh_token: secrets.REFRESH_TOKEN,
          client_id: secrets.CLIENT_ID,
          client_secret: secrets.SP_CLIENT_SECRET,
        },
        { timeout: HTTP_TIMEOUTS_MS.auth }
      );

      cachedAccessToken = data.access_token;
      accessTokenExpiresAt = Date.now() + data.expires_in * 1000;
      return cachedAccessToken;
    } finally {
      inflightTokenPromise = null;
    }
  })();
  return inflightTokenPromise;
}

function buildHeaders(accessToken) {
  return {
    'Content-Type': 'application/json',
    Accept: 'application/json',
    'x-amz-access-token': accessToken,
  };
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

async function createReport(headers) {
  const { data } = await axios.post(
    `${process.env.BASE_URL}/reports/2021-06-30/reports`,
    {
      marketplaceIds: [process.env.MARKETPLACE_ID],
      reportType: 'GET_MERCHANT_LISTINGS_ALL_DATA',
      dataStartTime: '2024-04-10T20:11:24.000Z',
    },
    { headers, timeout: HTTP_TIMEOUTS_MS.reports }
  );
  return data.reportId;
}

async function waitForReport(reportId, headers) {
  for (let attempt = 1; attempt <= REPORT_POLL_MAX_ATTEMPTS; attempt++) {
    const { data } = await axios.get(
      `${process.env.BASE_URL}/reports/2021-06-30/reports/${reportId}`,
      { headers, timeout: HTTP_TIMEOUTS_MS.reports }
    );
    const status = data.processingStatus;
    console.log(
      `Report ${reportId} status: ${status} (attempt ${attempt}/${REPORT_POLL_MAX_ATTEMPTS})`
    );

    if (status === 'DONE') return data.reportDocumentId;
    if (status === 'CANCELLED' || status === 'FATAL') {
      throw new Error(
        `Report ${reportId} ended in terminal state: ${status}`
      );
    }

    await new Promise((r) => setTimeout(r, REPORT_POLL_INTERVAL_MS));
  }
  throw new Error(
    `Report ${reportId} did not complete after ${REPORT_POLL_MAX_ATTEMPTS} attempts`
  );
}

async function getReportDocumentUrl(reportDocumentId, headers) {
  const { data } = await axios.get(
    `${process.env.BASE_URL}/reports/2021-06-30/documents/${reportDocumentId}`,
    { headers, timeout: HTTP_TIMEOUTS_MS.reports }
  );
  console.log(
    `Report document: compressionAlgorithm=${data.compressionAlgorithm || 'none'}`
  );
  return { url: data.url, compressionAlgorithm: data.compressionAlgorithm };
}

async function downloadAndParseReport({ url, compressionAlgorithm }) {
  const isGzip = compressionAlgorithm === 'GZIP';
  const { data } = await axios.get(url, {
    timeout: HTTP_TIMEOUTS_MS.reportDownload,
    responseType: isGzip ? 'arraybuffer' : 'text',
    // Axios auto-decompresses Content-Encoding: gzip but Amazon's pre-signed
    // S3 URLs serve the body raw — we must decompress manually below.
    decompress: false,
  });

  let text = isGzip
    ? zlib.gunzipSync(Buffer.from(data)).toString('utf-8')
    : data;

  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);

  const rows = text.split(/\r?\n/);
  if (rows.length === 0 || !rows[0]) {
    throw new Error('Inventory report is empty (no rows received)');
  }
  const headerCols = rows.shift().split('\t');
  const indices = {
    status: headerCols.indexOf('status'),
    price: headerCols.indexOf('price'),
    asin1: headerCols.indexOf('asin1'),
  };
  const missing = Object.entries(indices)
    .filter(([, idx]) => idx === -1)
    .map(([name]) => name);
  if (missing.length > 0) {
    console.error('Available headers:', headerCols);
    console.error(
      'First 300 chars of report data:',
      JSON.stringify(text.slice(0, 300))
    );
    throw new Error(
      `Inventory report missing required column(s): ${missing.join(', ')}. ` +
        `Got headers: [${headerCols.slice(0, 12).join(', ')}${
          headerCols.length > 12 ? ', ...' : ''
        }]`
    );
  }

  const seen = new Set();
  const asins = [];
  const prices = new Map();

  for (const row of rows) {
    if (!row) continue;
    const cols = row.split('\t');
    if (cols[indices.status] !== 'Active') continue;
    const asin = cols[indices.asin1];
    if (!asin || seen.has(asin)) continue;
    seen.add(asin);
    asins.push(asin);
    prices.set(asin, cols[indices.price]);
  }

  console.log(
    `Parsed ${asins.length} active unique ASINs from ${rows.length} report rows`
  );
  return { asinChunks: chunk(asins, ASIN_CHUNK_SIZE), prices };
}

async function fetchCatalogItems(asinChunks, headers) {
  const responses = await Promise.all(
    asinChunks.map((asinChunk) =>
      axios.get(`${process.env.BASE_URL}/catalog/2022-04-01/items`, {
        headers,
        timeout: HTTP_TIMEOUTS_MS.catalog,
        params: {
          identifiers: asinChunk.join(','),
          identifiersType: 'ASIN',
          pageSize: ASIN_CHUNK_SIZE,
          marketplaceIds: process.env.MARKETPLACE_ID,
          includedData: 'summaries,images,attributes',
        },
      })
    )
  );
  return responses.flatMap((r) => r.data.items || []);
}

function mergePrices(items, prices) {
  for (const item of items) {
    item.price = prices.get(item.asin);
  }
}

async function uploadToS3(items) {
  await S3.putObject({
    Bucket: process.env.S3_BUCKET_NAME,
    Key: 'products',
    Body: JSON.stringify(items),
    ContentType: 'application/json; charset=utf-8',
  }).promise();
}

function logAxiosError(err, label) {
  if (err.response) {
    console.error(
      `[${label}] status ${err.response.status}:`,
      err.response.data
    );
  } else if (err.request) {
    console.error(`[${label}] no response received`);
  } else {
    console.error(`[${label}] setup error:`, err.message);
  }
}

const app = express();
app.use(bodyParser.json());
app.use(awsServerlessExpressMiddleware.eventContext());

app.use(function (req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  next();
});

app.get('/getInventory', async (req, res) => {
  try {
    const accessToken = await getAccessToken();
    const headers = buildHeaders(accessToken);
    const reportId = await createReport(headers);
    const reportDocumentId = await waitForReport(reportId, headers);
    const document = await getReportDocumentUrl(reportDocumentId, headers);
    const { asinChunks, prices } = await downloadAndParseReport(document);
    const items = await fetchCatalogItems(asinChunks, headers);
    mergePrices(items, prices);
    await uploadToS3(items);
    console.log(`Uploaded ${items.length} items to S3`);
    res.sendStatus(200);
  } catch (err) {
    logAxiosError(err, 'getInventory');
    res.status(500).json({ error: err.message || 'Internal error' });
  }
});

app.get('/inventory/*', function (req, res) {
  res.json({ success: 'get call succeed!', url: req.url });
});

module.exports = app;
