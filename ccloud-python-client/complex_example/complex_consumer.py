import time
import logging
from confluent_kafka import Producer, Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("KafkaMasterclass")

def read_config():
    config = {}
    try:
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
    except FileNotFoundError:
        logger.warning("client.properties not found. Defaulting to local broker setup.")
    return config

class RobustConsumer:
    def __init__(self, base_config, group_id="academy-consumer-group"):
        self.config = base_config.copy()
        self.config.update({
            'group.id': group_id,
            'auto.offset.reset': 'earliest',     # Read from beginning if no committed offsets exist
            'enable.auto.commit': False,         # Manual offsets commit strategy for exact processing control
            'session.timeout.ms': 45000,         # Heartbeat drop tolerance limit
        })
        
        logger.info(f"Initializing Robust Consumer under Group ID: '{group_id}'...")
        self.consumer = Consumer(self.config)

    def consume_loop(self, topics):
        try:
            self.consumer.subscribe(topics)
            logger.info(f"Successfully subscribed to topics: {topics}. Polling for events...")
            # Always poll in a loop to maintain consumer liveness and trigger callbacks
            while True:
                # Poll block window set to 1.0 seconds
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue # No data available in this poll window
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event (Informational, not a failure)
                        logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"Critical Consumer Error encountered: {msg.error()}")
                    continue

                # Process valid message payloads
                key = msg.key().decode('utf-8') if msg.key() else "None"
                value = msg.value().decode('utf-8') if msg.value() else "None"
                
                logger.info(
                    f"Processed -> Key: {key:12} | Value: {value:28} | "
                    f"Partition: {msg.partition()} | Offset: {msg.offset()}"
                )
                
                # Perform Synchronous Commit after successful processing logic completes (At-Least-Once Strategy)
                try:
                    self.consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error(f"Failed to commit offset sequence: {e}")

        except KeyboardInterrupt:
            logger.warning("Shutdown signal received via CLI.")
        finally:
            # Leave the group cleanly, rebalancing other active workers immediately
            logger.info("Executing graceful disconnection protocol from consumer group...")
            self.consumer.close()

def main():
    config = read_config()
    target_topic = "kafka_demo_class_2"


    consumer_node = RobustConsumer(config)
    consumer_node.consume_loop([target_topic])


if __name__ == "__main__":
    main()