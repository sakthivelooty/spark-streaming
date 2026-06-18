# create a topic with 3 partitions
- `bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic third_topic --create --partitions 3`

# start one consumer
- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic third_topic --group my-first-application`

# start one producer and start producing
- `bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner --topic third_topic`

# start another consumer part of the same group. See messages being spread
- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic third_topic --group my-first-application`

# start another consumer part of a different group from beginning
- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic third_topic --group my-second-application --from-beginning`