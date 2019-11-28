"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self,producer_id,topic_name,key_schema,value_schema=None,num_partitions=1,num_replicas=1):

        """Initializes a Producer object with basic settings"""
        self.id = producer_id
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # broker properties
        self.broker_properties = {
            "broker.id": "0", 
            "log.dirs": "/logs/kafka/brokerlogs", 
            "zookeeper.connect": "http://localhost:2181", 
            "kafka": ["PLAINTEXT://localhost:9092","PLAINTEXT://localhost:9093","PLAINTEXT://localhost:9094"], 
            "schema_registry": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # self.producer = AvroProducer(
        # )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        
        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
        topic_list = []
        topic_list.append(NewTopic("example_topic", 1, 1))
        admin_client.create_topics(topic_list)

        logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
