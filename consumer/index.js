// 0.1) initilze module for server
const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  clientId: "mytest",
  logLevel: logLevel.INFO,
  waitForLeaders: true,
  brokers: ["localhost:9092"],
});
// 0.1) kafka logger for log 
kafka.logger().setLogLevel(logLevel.WARN);

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "kafka212341" });

// 1) this function for produce message for kafka 
async function produce(topic, value) {
  try {
    await producer.connect();

    await producer.send({
      topic,
      messages: [{ value }],
    });

    await producer.disconnect();
  } catch (err) {
    console.log(err);
  }
}

// 2) this function for consume message for kafka
async function consume(topic) {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic });

    await consumer.run({
      eachMessage: async ({ message }) => {
        console.log({
          value: message.value.toString(),
        });
      },
    });
  } catch (err) {
    console.log(err);
  }
}

consume("name");

produce("lastName", "hasanzadeh");
