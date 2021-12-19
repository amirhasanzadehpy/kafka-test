const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  waitForLeaders: true,
  brokers: ["localhost:9092"],
});

kafka.logger().setLogLevel(logLevel.WARN);

const producer = kafka.producer();

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

const consumer = kafka.consumer({ groupId: "server1231" });
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
