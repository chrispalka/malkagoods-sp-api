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

const S3 = new AWS.S3();
const secretsClient = new SecretsManagerClient({ region: 'us-east-1' });

const SECRET_NAME = 'malkagoods/sp-api-secret';
const SECRETS_TTL_MS = 5 * 60 * 1000;
const ACCESS_TOKEN_SAFETY_MARGIN_MS = 60 * 1000;
const REPORT_POLL_INTERVAL_MS = 10_000;
const REPORT_POLL_MAX_ATTEMPTS = 60;
const ASIN_CHUNK_SIZE = 20;

let cachedSecrets = null;
let secretsExpiresAt = 0;
let cachedAccessToken = null;
let accessTokenExpiresAt = 0;

async function getSecrets() {
  const now = Date.now();
  if (cachedSecrets && now < secretsExpiresAt) return cachedSecrets;

  const response = await secretsClient.send(
    new GetSecretValueCommand({
      SecretId: SECRET_NAME,
      VersionStage: 'AWSCURRENT',
    })
  );
  const parsed = response.SecretString ? JSON.parse(response.SecretString) : {};

  // Env-var fallback lets the cutover to Secrets Manager happen without downtime:
  // keep REFRESH_TOKEN/CLIENT_ID env vars populated until the secret is updated.
  cachedSecrets = {
    SP_CLIENT_SECRET: parsed.SP_CLIENT_SECRET,
    REFRESH_TOKEN: parsed.REFRESH_TOKEN || process.env.REFRESH_TOKEN,
    CLIENT_ID: parsed.CLIENT_ID || process.env.CLIENT_ID,
  };
  secretsExpiresAt = now + SECRETS_TTL_MS;
  return cachedSecrets;
}

async function getAccessToken() {
  const now = Date.now();
  if (
    cachedAccessToken &&
    now < accessTokenExpiresAt - ACCESS_TOKEN_SAFETY_MARGIN_MS
  ) {
    return cachedAccessToken;
  }

  console.log('LWA cache miss — minting new access token');
  const secrets = await getSecrets();
  const { data } = await axios.post(process.env.AUTH_URL, {
    grant_type: 'refresh_token',
    refresh_token: secrets.REFRESH_TOKEN,
    client_id: secrets.CLIENT_ID,
    client_secret: secrets.SP_CLIENT_SECRET,
  });

  cachedAccessToken = data.access_token;
  accessTokenExpiresAt = now + data.expires_in * 1000;
  return cachedAccessToken;
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
    { headers }
  );
  return data.reportId;
}

async function waitForReport(reportId, headers) {
  for (let attempt = 1; attempt <= REPORT_POLL_MAX_ATTEMPTS; attempt++) {
    const { data } = await axios.get(
      `${process.env.BASE_URL}/reports/2021-06-30/reports/${reportId}`,
      { headers }
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
    { headers }
  );
  return data.url;
}

async function downloadAndParseReport(url) {
  const { data } = await axios.get(url);
  const rows = data.split('\n');
  const headerCols = rows.shift().split('\t');
  const statusIdx = headerCols.indexOf('status');
  const priceIdx = headerCols.indexOf('price');
  const asinIdx = headerCols.indexOf('asin1');

  const seen = new Set();
  const asins = [];
  const prices = new Map();

  for (const row of rows) {
    if (!row) continue;
    const cols = row.split('\t');
    if (cols[statusIdx] !== 'Active') continue;
    const asin = cols[asinIdx];
    if (!asin || seen.has(asin)) continue;
    seen.add(asin);
    asins.push(asin);
    prices.set(asin, cols[priceIdx]);
  }

  return { asinChunks: chunk(asins, ASIN_CHUNK_SIZE), prices };
}

async function fetchCatalogItems(asinChunks, headers) {
  const responses = await Promise.all(
    asinChunks.map((asinChunk) =>
      axios.get(`${process.env.BASE_URL}/catalog/2022-04-01/items`, {
        headers,
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
    const docUrl = await getReportDocumentUrl(reportDocumentId, headers);
    const { asinChunks, prices } = await downloadAndParseReport(docUrl);
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
