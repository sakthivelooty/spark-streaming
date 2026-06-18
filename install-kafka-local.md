# Install Kafka 4.0.3
## Step 1: Install Java 17+ for Kafka 4.0.3
 - java --version

 should return the version of java
## Step 2: Install Kafka 
- `wget https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz`
- `tar -xzf kafka_2.13-3.0.0.tgz`
- `mv kafka_2.13-3.0.0 ~`
## Step 3 : change Bash file
- `nano ~/.bashrc`


add the following to the end the
- `path="$PATH:~/kafka/kafka_2.13-3.1.0/bin`
## Step 4: Generate cluster ID
- ` KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"`
## Step 5 set kafka logs
- `bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties`
## Step 6 Start kafka
- `bin/kafka-server-start.sh config/server.properties`


# work with Kafka
## Step 1: Create a topic to store your events
- `bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`

- Create topic: `bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic quickstart-events --partitions 3 --replication-factor 3`

- List topics : `bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`

- Describe topic : `bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic quickstart-events`

- Add partitions ; `bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic quickstart-events --partitions 3`

- Delete topic : `bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic quickstart-events`



## Step 2: Write some events into the topic
-  `bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092`

type some events every line is considered a event 
in the new terminal start the consumer to consume data

## Read the events
- `bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092`

# Kafka produce with key 
- `bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic --property parse.key=true --property key.separator=:`


>example key:example value

>name:John


# 