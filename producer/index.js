// :)
// 1) initialze module for kafka
const { Kafka, logLevel } = require("kafkajs");

// 2) make new kafka
const kafka = new Kafka({
  clientId: "my-app",
  waitForLeaders: true,
  brokers: ["localhost:9092"],
});

// 3) logger for kafka
kafka.logger().setLogLevel(logLevel.WARN);

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "kafka12134" });

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

produce("name", "amir");
consume("lastName");
