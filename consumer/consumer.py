import os
import sys
import time
import json
import signal
from pathlib import Path
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from fastavro import writer

TOPICS = ["orders.created", "orders.completed"]
OUT_DIR = Path("OUT_DIR", "data_avro")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load local schema files to use when writing avro containers
_schema_dir = Path(__file__).parent.parent / "schemas"
_topic_to_schema = {}

fname = "order_created.avsc"
p = _schema_dir / fname
if p.exists():
    with open(p, "r") as fh:
        _topic_to_schema[TOPICS[0]] = json.load(fh)

fname = "order_completed.avsc"
p = _schema_dir / fname
if p.exists():
    with open(p, "r") as fh:
        _topic_to_schema[TOPICS[1]] = json.load(fh)

# --- Schema Registry Client Setup ---
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Use AvroDeserializer which will read schema from registry using Confluent wire format
_avro_deserializers = {topic: AvroDeserializer(schema_registry_client, json.dumps(_topic_to_schema[topic])) for topic in TOPICS}

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "avro-file-writer-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe(TOPICS)

buffers = {topic: [] for topic in TOPICS}
FLUSH_COUNT = 100  # write file after this many records per topic
FLUSH_INTERVAL = 30  # seconds
_last_flush = time.time()
_running = True


def write_avro_file(topic):
    records = buffers[topic]
    if not records:
        return
    schema = _topic_to_schema.get(topic)
    if schema is None:
        print(f"No local schema for topic {topic}; skipping write")
        return
    fname = OUT_DIR / f"{topic.replace('.', '_')}_{int(time.time())}.avro"
    with open(fname, "wb") as out:
        writer(out, schema, records)
    print(f"Wrote {len(records)} records to {fname}")
    buffers[topic].clear()


def _flush_everything():
    for t in list(buffers.keys()):
        write_avro_file(t)


def sigint_handler(signum, frame):
    global _running
    print("Received signal, flushing buffers and exiting...")
    _running = False


signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGTERM, sigint_handler)

print(f"Starting consumer for topics: {TOPICS}")
try:
    while _running:
        msg = consumer.poll(1.0)
        now = time.time()
        if not msg:
            # periodic flush by time
            if now - _last_flush > FLUSH_INTERVAL:
                _flush_everything()
                _last_flush = now
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        topic = msg.topic()
        try:
            value = _avro_deserializers[topic](msg.value(), SerializationContext(topic, MessageField.VALUE))
        except Exception as e:
            print(f"Failed to deserialize message from {topic}: {e}")
            continue
        buffers.setdefault(topic, []).append(value)
        # flush by count
        if len(buffers[topic]) >= FLUSH_COUNT:
            write_avro_file(topic)
        # periodic time flush
        if now - _last_flush > FLUSH_INTERVAL:
            _flush_everything()
            _last_flush = now

finally:
    print("Closing consumer and flushing remaining records...")
    try:
        _flush_everything()
    except Exception as e:
        print(f"Error while flushing: {e}")
    consumer.close()
    print("Done.")
