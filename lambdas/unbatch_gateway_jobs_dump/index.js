console.log("Loading function");

const aws = require("aws-sdk");

const s3 = new aws.S3({ apiVersion: "2006-03-01" });

exports.handler = async (event, context) => {
  // Get the object from the event and show its content type
  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, " ")
  );
  const params = {
    Bucket: bucket,
    Key: key
  };
  try {
    // fetch gateway jobs dump from s3
    const data = await s3.getObject(params).promise();
    let batchEvents = JSON.parse(data.Body.toString("utf-8"));
    let unBatchedEvents = [];
    batchEvents.forEach(batchedEvent => {
      batchedEvent.event_payload.batch.forEach(ev => {
        unBatchedEvents.push(ev.rl_message);
      });
    });
    let putParams = {
      Body: JSON.stringify(unBatchedEvents),
      Bucket: `unbatched_${bucket}`,
      Key: key
    };
    // write unbatched jobs dump to s3
    await s3.putObject(putParams).promise();
    return "Success";
  } catch (err) {
    console.log(err);
    const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
    console.log(message);
    throw new Error(message);
  }
};
