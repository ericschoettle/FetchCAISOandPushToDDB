#!/usr/bin/env node

//zip -r index.zip *
//aws lambda update-function-code --function-name FetchCAISOAndPushToSQS --zip-file fileb://index.zip




const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB({apiVersion: '2012-08-10', region: 'us-east-1' });
const http = require('http');
const csv = require('csv-parser');
const unzipper = require('unzipper');
const concat = require('concat-stream');
const algorithm = require('./algorithm.js');
const moment = require('moment');


const fetchAndTransform = async (event) => {
  const startDate = event.iterator.startDate || 20160101;
  const endDate = event.iterator.endDate || 201601030;
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

      let hours = []; 
      resp.pipe(unzipper.Parse()) // Unzip
        .on('entry', (entry)=>{
          // resolve({status: 200, position: 'unzip'})
          const fileName = entry.path;
          if (fileName.match(/\.csv/)) {
            entryCount ++;

            // Needs logic to go to CSV if CSV, and return xml as error if it gives XML (b/c that's what the API does when you ask for a CSV and there's an error)   
            entry.pipe(csv())
              .on('data', (data) => {
                if (data.LMP_TYPE === 'LMP') {
                  recordCount++
                  hours.push({
                    // 'ID': {S: `${data.NODE}-${data.OPR_DT}-${data.OPR_HR}`},
                    'nodeId': data.NODE,
                    'date': data.OPR_DT,
                    'operatingHour': data.OPR_HR,
                    'startTimeGMT': data.INTERVALSTARTTIME_GMT,
                    'endTimeGMT': data.INTERVALENDTIME_GMT,
                    'price': data.MW,
                    'duration': '60',
                  });
                } 
              })
              .on('end', ()=>{
                // sort hours
                hours.sort((prev,curr)=>{
                  return new Date(prev.startTimeGMT) - new Date(curr.startTimeGMT)
                })
                // add prices
                hours = algorithm(hours, 0, 3);
                debugger;
                // PUT to dynamoDB
                for (let i = 0; i < hours.length; i++) {
                  if (i % 24 === 24 - 1) { // full day
                    let day = hours.slice(i - 23, i + 1)
                    const createDate = moment().format();
                    let putBatch = day.map(hour=>{
                      let item = {
                        'nodeId': {S: hour.nodeId},
                        'date': {S: hour.date},
                        'operatingHour': {N: hour.operatingHour},
                        'startTimeGMT': {S: hour.startTimeGMT},
                        'endTimeGMT': {S: hour.endTimeGMT},
                        'price': {N: hour.price},
                        'duration': {N: '60'},
                        'createDate': {S: createDate},
                      }
                      if (hour.transaction) {
                        item.marginalHour = {N: hour.marginalHour.toString()};
                        item.transaction = {S: hour.transaction};
                      }
                      return {PutRequest: {
                        Item: item
                      }}
                    });
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
                  }       
                }

                // serve response
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

console.log(Promise.resolve(fetchAndTransform({iterator: {}})));

exports.fetchAndTransform = fetchAndTransform;