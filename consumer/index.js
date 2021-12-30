// 0.1) initilze module for server
const { Kafka, logLevel, CompressionTypes } = require("kafkajs");
const ip = require("ip");
const host = process.env.HOST_IP || ip.address();

const kafka = new Kafka({
  clientId: "est",
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
});

// 0.1) kafka logger for log
kafka.logger().setLogLevel(logLevel.ERROR);

const producer = kafka.producer();
// const consumer = kafka.consumer({ groupId: "kafka212341" });

// const produceMessage = async (topic) => {
//   try {
//     await producer.send({
//       topic,
//       messages: [{ value: "horse" }],
//     });
//   } catch (err) {
//     console.error;
//   }
// };

// setInterval(produceMessage, 2);

// 1) this function for produce message for kafka
const run = async () => {
  await producer.connect();

  await producer.send({
    topic: "animal",
    compression: CompressionTypes.GZIP,
    messages: [{ value: "horse" }],
  });

  console.log("send");
  await producer.disconnect();
};
// setInterval(run, 22);
run().catch(console.error);
