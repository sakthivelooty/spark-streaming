# create a topic with 3 partitions
- `bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic second_topic --create --partitions 3`

# consuming
- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic`

# other terminal
- `bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner --topic second_topic`


# consuming from beginning
- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic --from-beginning`

# display key, values and timestamp in consumer
- `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --property print.partition=true --from-beginning`

