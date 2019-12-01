"""Creates a turnstile data producer"""
import logging
from pathlib import Path
import random
import string

from confluent_kafka import avro

from producer import Producer 
from turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        self.turnstile_id = "turnstile-" + station_name + "-" + ''.join([random.choice(string.ascii_letters + string.digits) for n in range(6)]).lower()
        self.topic_name = self.turnstile_id
        super().__init__(
            self.turnstile_id, 
            self.topic_name, 
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=2
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        try: 
            num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
            self.avro_producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                "station_id": self.station.station_id, 
                "station_name": self.station.station_name, 
                "line": num_entries
            }
            )
        except Exception as e: 
           logger.info("Turnstile message for {} failed to write to topic {} with exception {} - skipping".format(self.turnstile_id, self.topic_name, e))

