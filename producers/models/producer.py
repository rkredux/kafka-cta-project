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
        #TODO set the client id in a better way
        #TODO #Async vs Sync Writes and let's read this documentation more thoroughly to understand the difference. 
        #https://docs.confluent.io/4.0.0/clients/producer.html
        #Finally let's test this producer before committing the changes
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

        #Configure the AvroProducer
        self.avro_producer = AvroProducer({
            'bootstrap.servers': self.broker_properties["kafka"], 
            'schema.registry.url': self.broker_properties["schema_properties"]}, 
            self.key_schema, 
            self.value_schema)


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
        topic_list = [NewTopic(self.topic_name,self.num_partitions,self.num_replicas)]
        fs = admin_client.create_topics(topic_list)
        for topic, f in fs.items(): 
            try: 
                f.result()
                print("Topic {} created".format(topic))
                Producer.existing_topics.add(self.topic_name)
            except Exception as e: 
                logger.info("Failed to create topic {}: {}".format(topic, e))

    def time_millis(self):
        return int(round(time.time() * 1000))

    #TODO What is this and where is it going to be used?
    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("producer close incomplete - skipping")

