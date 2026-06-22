import time
import logging
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("KafkaMasterclass")

def read_config():
    config = {}
    try:
        with open("./client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
    except FileNotFoundError:
        logger.warning("client.properties not found. Defaulting to local broker setup.")
    return config


class AdvancedProducer:
    def __init__(self, base_config):
        # Merge base configs with strict enterprise delivery guarantees
        self.config = base_config.copy()
        self.config.update({
            'acks': 'all',                      # Ensures data durability (all in-sync replicas must acknowledge)
            'enable.idempotence': True,         # Protects against duplicates from network retries
            'max.in.flight.requests.per.connection': 5, # High throughput while maintaining message ordering
            'compression.type': 'snappy',       # Reduces network load and broker storage footprint
            'linger.ms': 20,                    # Batches messages for 20ms to improve overall throughput
            'batch.num.messages': 1000          # Maximum number of messages per batch
        })
        
        logger.info("Initializing Advanced Producer with Idempotence and Acks=All...")
        self.producer = Producer(self.config)

    def delivery_report(self, err, msg):
        """Asynchronous callback executed by poll() once the broker acknowledges receipt."""
        if err is not None:
            logger.error(f"Delivery failed for record key {msg.key()}: {err}")
        else:
            logger.info(
                f"Successfully Acknowledged! -> Topic: {msg.topic()} | "
                f"Partition: [{msg.partition()}] | Offset: {msg.offset()}"
            )

    def publish_messages(self, topic, total_messages=10):
        logger.info(f"Starting execution of {total_messages} records to topic: '{topic}'")
        
        for i in range(total_messages):
            # Formulating keys to demonstrate Key-Based Partitioning
            # Even keys will route to one partition, odd keys to another.
            key = f"user_id_2_{i}" 
            value = f"transaction_payload_data_{i}"
            
            try:
                # Asynchronous delivery trigger
                self.producer.produce(
                    topic=topic,
                    key=key.encode('utf-8'),
                    value=value.encode('utf-8'),
                    callback=self.delivery_report  # Attaching the async completion callback
                )
                
                # Essential: Serve the internal queue events to trigger delivery reports
                # poll(0) is non-blocking; it returns immediately after draining events
                self.producer.poll(0)
                time.sleep(0.1) # Simulate a steady stream of data
                
            except BufferError:
                logger.warning("Local client buffer overflow detected. Blocking thread for 1 second...")
                self.producer.poll(1.0)
            except Exception as e:
                logger.error(f"Critical error during local buffering: {e}")

        # Block execution until all remaining background messages are completely flushed
        logger.info("Awaiting final broker confirmations (Flushing client buffers)...")
        self.producer.flush()

def main():
    config = read_config()
    print(config)
    target_topic = "kafka_demo_class"

    # Step 1: Demonstrate Advanced Producing Mechanics
    producer_node = AdvancedProducer(config)
    producer_node.publish_messages(target_topic, total_messages=15)


if __name__ == "__main__":
    main()