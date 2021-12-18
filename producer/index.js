console.log("producer....");

import Kafka from "node-rdkafka";

const stream = Kafka.Producer.createWriteStream(
  {
    "metadata.broker.list": "localhost:9092",
  },
  {},
  { topic: "test" }
);

function queueMessage() {
  const success = stream.write(Buffer.from("hi"));
  if (success) {
    console.log("message wrote successfylly to stream");
  } else {
    console.log("somthing went wrong");
  }
}

setInterval(() => {
  queueMessage();
}, 3000);
