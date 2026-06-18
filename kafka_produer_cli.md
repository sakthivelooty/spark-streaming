# kafka-console-producer.sh
## a cli command for the kafka console producer 

-  `bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092`

## Producing with properties
- `bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic quickstart-events --producer-property acks=all`

## Producing to non-existing topic
- `bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic new_topic`

###### our new topic only has 1 partition
- `kafka-topics.sh --bootstrap-server localhost:9092 --list`
- `kafka-topics.sh --bootstrap-server localhost:9092 --topic new_topic --describe`

## produce with keys
- `bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic quickstart-events --property parse.key=true --property key.separator=:`

  - example key:example value
  - name:Stephane

