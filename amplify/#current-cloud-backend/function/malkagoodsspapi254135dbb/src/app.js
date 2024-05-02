/*
Use the following code to retrieve configured secrets from SSM:

*/
// Use this code snippet in your app.
// If you need more information about configurations or implementing the sample code, visit the AWS docs:
// https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started.html

const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require('@aws-sdk/client-secrets-manager');

const AWS = require('aws-sdk');
const S3 = new AWS.S3();

const secret_name = 'malkagoods/sp-api-secret';

const client = new SecretsManagerClient({
  region: 'us-east-1',
});

const getSecret = async () => {
  try {
    return await client.send(
      new GetSecretValueCommand({
        SecretId: secret_name,
        VersionStage: 'AWSCURRENT', // VersionStage defaults to AWSCURRENT if unspecified
      })
    );
  } catch (error) {
    // For a list of exceptions thrown, see
    // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    throw error;
  }
};

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

const express = require('express');
const bodyParser = require('body-parser');
const awsServerlessExpressMiddleware = require('aws-serverless-express/middleware');
const axios = require('axios');

// declare a new express app
const app = express();
app.use(bodyParser.json());
app.use(awsServerlessExpressMiddleware.eventContext());

// Enable CORS for all methods
app.use(function (req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  next();
});

app.get('/getInventory', async function (req, res) {
  const secretValue = await getSecret();
  const spClientSecret = secretValue.SecretString
    ? JSON.parse(secretValue.SecretString)['SP_CLIENT_SECRET']
    : null;
  let accessToken;

  const body = {
    grant_type: 'refresh_token',
    refresh_token: process.env.REFRESH_TOKEN,
    client_id: process.env.CLIENT_ID,
    client_secret: spClientSecret,
  };

  let priceArray = [];

  // get access token
  axios
    .post(process.env.AUTH_URL, body)
    .then((response) => {
      accessToken = response.data.access_token;

      // Once the access token is obtained, proceed with API calls
      const headers = {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        'x-amz-access-token': accessToken,
      };

      const chunkArr = (arr, chunk) => {
        const results = [];
        const tmpArr = arr.slice();

        while (tmpArr.length) {
          results.push(tmpArr.splice(0, chunk));
        }
        return results;
      };

      const parseData = (url) => {
        return axios(url).then((response) => {
          const result = [];
          const data = response.data;
          // Split data into rows
          const rows = data.split('\n');

          // Extract column headers
          const statusIndex = rows[0].split('\t').indexOf('status');
          const priceIndex = rows[0].split('\t').indexOf('price');
          const asinIndex = rows.shift().split('\t').indexOf('asin1');
          rows.forEach((row) => {
            if (
              row.split('\t')[statusIndex] === 'Active' &&
              result.indexOf(row.split('\t')[asinIndex]) === -1
            ) {
              result.push(row.split('\t')[asinIndex]);
              priceArray.push({
                asin: row.split('\t')[asinIndex],
                price: row.split('\t')[priceIndex],
              });
            }
          });
          return chunkArr(result, 20);
        });
      };

      const uploadJSON = async (items) => {
        try {
          const params = {
            Bucket: process.env.S3_BUCKET_NAME,
            Key: 'products',
            Body: JSON.stringify(items),
            ContentType: 'application/json; charset=utf-8',
          };
          await S3.putObject(params).promise();
          console.log('Upload Complete');
          res.sendStatus(200);
        } catch (e) {
          console.log('Upload Error: ', e);
          res.status(422).json({ error: 'Error uploading to S3' });
        }
      };

      const getItems = async (asinArr) => {
        const tempArr = [];
        const promises = asinArr.map((arrChunk) => {
          return axios(
            `${
              process.env.BASE_URL
            }/catalog/2022-04-01/items?identifiers=${arrChunk.join()}&identifiersType=ASIN&pageSize=20&marketplaceIds=${
              process.env.MARKETPLACE_ID
            }&includedData=summaries,images,attributes`,
            {
              headers,
            }
          );
        });
        try {
          const responses = await Promise.all(promises);
          responses.forEach((response) => {
            tempArr.push(response.data.items);
          });
          const resultSet = [].concat(...tempArr);
          resultSet.forEach((product) => {
            product['price'] = priceArray.find(
              (item) => item.asin === product.asin
            )?.price;
          });
          uploadJSON(resultSet);
        } catch (error) {
          console.error('Error: ', error);
          throw error;
        }
      };

      const getReportDocument = (reportDocId) => {
        axios(
          `${process.env.BASE_URL}/reports/2021-06-30/documents/${reportDocId}`,
          {
            headers,
          }
        )
          .then((response) => {
            parseData(response.data.url).then((items) => {
              getItems(items);
            });
          })
          .catch((error) => {
            if (error.response) {
              console.error(
                'Server responded with status code:',
                error.response.status
              );
              console.error('Response data:', error.response.data);
            } else if (error.request) {
              console.error('No response received:', error.request);
            } else {
              console.error('Error setting up the request:', error.message);
            }
            res.status(500).json({ error: 'Error fetching report document' });
          });
      };

      const getReportDocId = (reportId) => {
        axios(
          `${process.env.BASE_URL}/reports/2021-06-30/reports/${reportId}`,
          {
            headers,
          }
        )
          .then((response) => {
            if (response.data.processingStatus === 'DONE') {
              getReportDocument(response.data.reportDocumentId);
            } else {
              console.log('Report in progress...');
              setTimeout(() => {
                getReportDocId(response.data.reportId);
              }, 10000);
            }
            console.log(response.data);
          })
          .catch((error) => {
            if (error.response) {
              console.error(
                'Server responded with status code:',
                error.response.status
              );
              console.error('Response data:', error.response.data);
            } else if (error.request) {
              console.error('No response received:', error.request);
            } else {
              console.error('Error setting up the request:', error.message);
            }
            res
              .status(500)
              .json({ error: 'Error fetching report document ID' });
          });
      };

      const createReport = () => {
        const data = {
          marketplaceIds: [process.env.MARKETPLACE_ID],
          reportType: 'GET_MERCHANT_LISTINGS_ALL_DATA',
          dataStartTime: '2024-04-10T20:11:24.000Z',
        };
        axios
          .post(`${process.env.BASE_URL}/reports/2021-06-30/reports`, data, {
            headers,
          })
          .then((response) => {
            getReportDocId(response.data.reportId);
          })
          .catch((error) => {
            if (error.response) {
              console.error(
                'Server responded with status code:',
                error.response.status
              );
              console.error('Response data:', error.response.data);
            } else if (error.request) {
              console.error('No response received:', error.request);
            } else {
              console.error('Error setting up the request:', error.message);
            }
            res.status(500).json({ error: 'Error creating report' });
          });
      };
      createReport();
    })
    .catch((error) => {
      if (error.response) {
        console.error(
          'Server responded with status code:',
          error.response.status
        );
        console.error('Response data:', error.response.data);
      } else if (error.request) {
        console.error('No response received:', error.request);
      } else {
        console.error('Error setting up the request:', error.message);
      }
      res.status(500).json({ error: 'Error fetching token' });
    });
});

app.get('/inventory/*', function (req, res) {
  // Add your code here
  res.json({ success: 'get call succeed!', url: req.url });
});

module.exports = app;
