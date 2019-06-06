#!/usr/bin/env node

//zip -r index.zip *
//aws lambda update-function-code --function-name FetchCAISOAndPushToSQS --zip-file fileb://index.zip

// OR: http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=20180919T07:00-0000&enddatetime=20180920T07:00-0000&version=1&market_run_id=DAM&node=LAPLMG1_7_B2&resultformat=6

// exports.handler = async (event) => {
//   // TODO implement
//   const url = `http://oasis.caiso.com/oasisapi/GroupZip?groupid=DAM_LMP_GRP&startdatetime=20170920T07:00-0000&enddatetime=20170921T08:00-0000&version=1&resultformat=6`;

//   const http = require('http');
//   const csv = require('csv-parser');
//   const unzipper = require('unzipper');
//   const QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/852994383641/triggersLambdatToPullFromOasis';
//   const AWS = require('aws-sdk');
//   const sqs = new AWS.SQS({region : 'us-east-1'});
    
//   return new Promise((resolve)=>{
//     let successCount = 0;
//     let failureCount = 0;
//     let dataCount = 0;

//     http.get(url, (resp)=>{
//       resp.on('error', function(err) {
//         console.log('Error while reading', err);
//       });

//       resp.pipe(unzipper.Parse())
//         .on('entry', (entry)=>{
//           let filename = entry.path;
//           if (filename.match(/PRC_LMP_DAM_LMP_v1.csv/)) {
//             var putBatch = []     
//             entry.pipe(csv())
//               .on('data', (data) => {
//                 dataCount++
//                 if (dataCount % 1000 === 0) {
//                   // console.log(data);
//                   console.log(dataCount);
//                 }
//                 // console.log(dataCount)
//                 putBatch.push({
//                   PutRequest: {
//                     Item: {
//                       'ID': {S: `${data.NODE}-${data.OPR_DT}-${data.OPR_HR}`},
//                       'nodeId': {S: data.NODE},
//                       'date': {S: data.OPR_DT},
//                       'operatingHour': {N: data.OPR_HR},
//                       'startTimeGMT': {S: data.INTERVALSTARTTIME_GMT},
//                       'endTimeGMT': {S: data.INTERVALSTARTTIME_GMT},
//                       'price': {N: data.MW},
//                       'duration': {N: '60'}
//                     }
//                   }
//                 });
//                 if (putBatch.length>23) {
//                   const params = {
//                     QueueUrl: QUEUE_URL,
//                     MessageBody: JSON.stringify({
//                       RequestItems: {
//                         'CA-Hours-DAM-LMP': putBatch
//                       }
//                     })
//                   }
//                   sqs.sendMessage(params, function(err,data){
//                     // if(err) {
//                     //   console.log('error:',"Fail Send Message" + failureCount);
//                     //   failureCount++
//                     //   // context.done('error', "ERROR Put SQS");  // ERROR with message
//                     // } else {
//                     //   console.log('sendMessage Success! packageCount:', successCount);
//                     //   successCount++
//                     //   // context.done(null,'');  // SUCCESS 
//                     // }
//                   });
//                   putBatch = [];
//                 }
//               })
//               .on('end', ()=>{
//                 console.log('end: ' + dataCount);
//                 const response = {
//                   statusCode: 200,
//                   body: {
//                     successCount: successCount,
//                     failureCount: failureCount
//                   },
//                 };
//                 console.log('end inner pipe - records: ' + dataCount)
//                 resolve(response);

//               })
//               .on('finish', ()=>{
//                 console.log('finish: ' + dataCount);
//                 const response = {
//                   statusCode: 200,
//                   body: {
//                     successCount: successCount,
//                     failureCount: failureCount
//                   },
//                 };
//                 console.log('finish inner pipe - records: ' + dataCount)
//                 resolve(response);

//               })
//               .on('error', (error)=>{
//                 console.log('ERROR!!!: ' + error)
//               });
//           } else {
//             entry.autodrain();
//           }
//         });
//     });
//   });
// };


const AWS = require('aws-sdk');
// AWS.config.update({region: 'REGION'});
const dynamoDB = new AWS.DynamoDB({apiVersion: '2012-08-10'});
const http = require('http');
const csv = require('csv-parser');
const unzipper = require('unzipper');
const xml = require('xml2js');
const parser = new xml.Parser();
const concat = require('concat-stream');
// const unzipper = require('adm-zip');
const moment = require('moment');


const fetchAndTransform = async (event) => {
  const startDate = event.iterator.startDate || 20160101;
  const endDate = event.iterator.endDate || 20160201;
  const nodeId = event.nodeId || 'LAPLMG1_7_B2';
  // Questions - for LAPLMG1_7_B2, the earlist I an start is around 15/11 - is this true for all nodes?
  // API only accepts one month at a time :/
  const url = `http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=${startDate}T07:00-0000&enddatetime=${endDate}T07:00-0000&version=1&market_run_id=DAM&node=${nodeId}&resultformat=6`;
  return new Promise((resolve, reject)=>{
    http.get(url, (resp)=>{
      resp.on('error', function(err) {
        console.log('Error on HTML get', err);
        // Need to get these to actually error out. 
        const response = {
          statusCode: 512,
          error: err,
          body: { endDate: endDate },
        };
        resolve(response);
      });

      let errors = [];
      let entryCount = 0;
      let recordCount = 0;
      let putBatchCount = 0;
      resp.pipe(unzipper.Parse()) // Unzip
        .on('entry', (entry)=>{
          // resolve({status: 200, position: 'unzip'})
          const fileName = entry.path;
          if (fileName.match(/\.csv/)) {
            entryCount ++;
            let putBatch = [];
            // Needs logic to go to CSV if CSV, and return xml as error if it gives XML (b/c that's what the API does when you ask for a CSV and there's an error)   
            entry.pipe(csv())
              .on('data', (data) => {
                if (data.LMP_TYPE === 'LMP') {
                  recordCount++
                  const createDate = moment().format();
                  putBatch.push({
                    PutRequest: {
                      Item: {
                        // 'ID': {S: `${data.NODE}-${data.OPR_DT}-${data.OPR_HR}`},
                        'nodeId': {S: data.NODE},
                        'date': {S: data.OPR_DT},
                        'operatingHour': {N: data.OPR_HR},
                        'startTimeGMT': {S: data.INTERVALSTARTTIME_GMT},
                        'endTimeGMT': {S: data.INTERVALSTARTTIME_GMT},
                        'price': {N: data.MW},
                        'duration': {N: '60'},
                        'createDate': {S: createDate}
                      }
                    }
                  });
                } 
                if (putBatch.length === 24) {
                  putBatchCount++
                  const params = {
                    RequestItems: {
                      'CA-Hours-DAM-LMP': putBatch
                    }
                  }
                  dynamoDB.batchWriteItem(params, (err, data)=>{
                    if (err) {
                      //TODO - RETURN an error type. Implement a repeat policy in step function. 
                      console.log(`Error on write to ddb: ${err}`);
                      errors.push(`Error on write to ddb: ${err}`);
                    } 
                  });
                  putBatch = [];
                } else if (putBatch.length > 24) {
                  console.log(`Error on put batch - length: ${putBatch.length}`);
                  errors.push(`Error on put batch - length: ${putBatch.length}`);
                } 
  
              })
              .on('end', ()=>{
                if (errors.length) {
                  const response = {
                    statusCode: 512,
                    body: { 
                      url: url,
                      endDate: endDate,
                      errors: JSON.stringify(errors),
                      entryCount: entryCount,
                      csvDataCount: csvDataCount,
                      recordCount: recordCount,
                      putBatchCount: putBatchCount
                    },
                  };
                  resolve(response);
                } else {
                  console.log('Success!!!')
                  const response = {
                    statusCode: 200,
                    body: { 
                      url: url,
                      endDate: endDate,
                      entryCount: entryCount,
                      recordCount: recordCount,
                      putBatchCount: putBatchCount
                    },
                  };
                  resolve(response);
                }
              })
              .on('error',(err) =>{
                console.log(`Error on CSV parse ${err}`);
                const response = {
                  statusCode: 512,
                  error: `Error on CSV parse ${err}`,
                  body: { endDate: endDate },
                };
                resolve(response);
              });
          } else { // Errors return XML
            entry.pipe(concat((data)=>{
              const err = data.toString().match(/\<m:ERR_DESC>(.*)\<\/m:ERR_DESC>/)[1];
              console.log(`Error in CAISO API call: ${err}`);
              const response = {
                statusCode: 512,
                error: `Error in CAISO API call ${err}`,
                body: { endDate: endDate },
              };
              resolve(response);
            }));
          }
        })
        .on('error',(err) =>{
          console.log(`Error on unzip: ${err}`);
          const response = {
            statusCode: 512,
            error: `Error on Unzip ${err}`,
            body: { endDate: endDate },
          };
          resolve(response);
        });
    });
  });
};

// console.log(Promise.resolve(fetchAndTransform({iterator: {}})));

exports.fetchAndTransform = fetchAndTransform;