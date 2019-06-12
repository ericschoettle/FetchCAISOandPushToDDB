// OR: http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=20180919T07:00-0000&enddatetime=20180920T07:00-0000&version=1&market_run_id=DAM&node=LAPLMG1_7_B2&resultformat=6


// this was for a previous architecture, using SQS rather than step functions. It took all nodes for a given day and pushed them to SQS to then write to dynamoDB. 
// This had two problems - first, SQS runs loosley in order, but has some parallelism. While maintaining order doesn't matter, this led to overwriting my DDB write capacity, which is spendy. 
// Second, going further down this path would have led to a whole set of lambdas and queues, etc. feeding eachother, that would not be particularly legible. Step functions provide basically the same functionality, but graph the path of the data, making it easier to understand. re2
exports.handler = async (event) => {
  // TODO implement
  const url = `http://oasis.caiso.com/oasisapi/GroupZip?groupid=DAM_LMP_GRP&startdatetime=20170920T07:00-0000&enddatetime=20170921T08:00-0000&version=1&resultformat=6`;

  const http = require('http');
  const csv = require('csv-parser');
  const unzipper = require('unzipper');
  const QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/852994383641/triggersLambdatToPullFromOasis';
  const AWS = require('aws-sdk');
  const sqs = new AWS.SQS({region : 'us-east-1'});
    
  return new Promise((resolve)=>{
    let successCount = 0;
    let failureCount = 0;
    let dataCount = 0;

    http.get(url, (resp)=>{
      resp.on('error', function(err) {
        console.log('Error while reading', err);
      });

      resp.pipe(unzipper.Parse())
        .on('entry', (entry)=>{
          let filename = entry.path;
          if (filename.match(/PRC_LMP_DAM_LMP_v1.csv/)) {
            var putBatch = []     
            entry.pipe(csv())
              .on('data', (data) => {
                dataCount++
                if (dataCount % 1000 === 0) {
                  // console.log(data);
                  console.log(dataCount);
                }
                // console.log(dataCount)
                putBatch.push({
                  PutRequest: {
                    Item: {
                      'ID': {S: `${data.NODE}-${data.OPR_DT}-${data.OPR_HR}`},
                      'nodeId': {S: data.NODE},
                      'date': {S: data.OPR_DT},
                      'operatingHour': {N: data.OPR_HR},
                      'startTimeGMT': {S: data.INTERVALSTARTTIME_GMT},
                      'endTimeGMT': {S: data.INTERVALSTARTTIME_GMT},
                      'price': {N: data.MW},
                      'duration': {N: '60'}
                    }
                  }
                });
                if (putBatch.length>23) {
                  const params = {
                    QueueUrl: QUEUE_URL,
                    MessageBody: JSON.stringify({
                      RequestItems: {
                        'CA-Hours-DAM-LMP': putBatch
                      }
                    })
                  }
                  sqs.sendMessage(params, function(err,data){
                    // if(err) {
                    //   console.log('error:',"Fail Send Message" + failureCount);
                    //   failureCount++
                    //   // context.done('error', "ERROR Put SQS");  // ERROR with message
                    // } else {
                    //   console.log('sendMessage Success! packageCount:', successCount);
                    //   successCount++
                    //   // context.done(null,'');  // SUCCESS 
                    // }
                  });
                  putBatch = [];
                }
              })
              .on('end', ()=>{
                console.log('end: ' + dataCount);
                const response = {
                  statusCode: 200,
                  body: {
                    successCount: successCount,
                    failureCount: failureCount
                  },
                };
                console.log('end inner pipe - records: ' + dataCount)
                resolve(response);

              })
              .on('finish', ()=>{
                console.log('finish: ' + dataCount);
                const response = {
                  statusCode: 200,
                  body: {
                    successCount: successCount,
                    failureCount: failureCount
                  },
                };
                console.log('finish inner pipe - records: ' + dataCount)
                resolve(response);

              })
              .on('error', (error)=>{
                console.log('ERROR!!!: ' + error)
              });
          } else {
            entry.autodrain();
          }
        });
    });
  });
};