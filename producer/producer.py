from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
import uuid
import datetime
import os
import random
import time


def main():
    # --- Schema Registry Client Setup ---
    schema_registry_conf = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    schema_path = os.path.join(os.path.dirname(__file__), "../schemas/order_created.avsc")
    with open(schema_path) as f:
        order_created_schema = f.read()

    schema_path = os.path.join(os.path.dirname(__file__), "../schemas/order_completed.avsc")
    with open(schema_path) as f:
        order_completed_schema = f.read()

    order_created_serializer = AvroSerializer(
        schema_registry_client,
        order_created_schema
    )

    order_completed_serializer = AvroSerializer(
        schema_registry_client,
        order_completed_schema
    )

    string_serializer = StringSerializer('utf_8')

    producer_conf = {
        "bootstrap.servers": "localhost:9092"
    }

    producer = Producer(producer_conf)

    # ---- Data Generators ----
    currencies = ["EUR", "USD", "GBP"]
    price_range = (5, 500)

    order_queue = {}

    def generate_order_created():
        return {
            "order_id": str(uuid.uuid4()),
            "customer_id": f"cust_{random.randint(1,1000)}",
            "price": round(random.uniform(*price_range), 2),
            "currency": random.choice(currencies),
            "created_at": datetime.datetime.utcnow().isoformat()
        }
    
    def generate_order_completed(order_id):
        return {
            "order_id": order_id,
            "completed_at": (datetime.datetime.utcnow()+datetime.timedelta(minutes=random.uniform(1,60))).isoformat(),
            "status": random.choice(["SUCCESS", "FAILED"])
        }
    
    def generate_broken_order():
        event = generate_order_created()
        if random.random() < 0.2:  # 20% chance to break
            event.pop("price")  # remove required field
        return event

    for _ in range(1000):
        producer.poll(0)
        
        # Produce order_created event
        if random.random() < 0.8:
            order_created_1 = generate_order_created()
        else:
            order_created_1 = generate_broken_order()
        print(f"Producing order_created event: {order_created_1}")
        try:
            producer.produce(
                topic="orders.created",
                key=string_serializer(order_created_1["order_id"]),
                value=order_created_serializer(order_created_1, SerializationContext("orders.created", MessageField.VALUE)),
            )
            # Track order for completion
            order_queue[order_created_1["order_id"]] = order_created_1
        except Exception as e:
            print(f"Dropped invalid event: {order_created_1}")
            print(f"Reason: {e}")
        
        # Randomly produce order_completed event for existing orders
        if order_queue and random.random() < 0.4:
            order_id = random.choice(list(order_queue.keys()))
            order_completed = generate_order_completed(order_id)
            print(f"Producing order_completed event: {order_completed}")
            try:
                producer.produce(
                    topic="orders.completed",
                    key=string_serializer(order_id),
                    value=order_completed_serializer(order_completed, SerializationContext("orders.completed", MessageField.VALUE)),
                )
                # Remove from queue after completion
                del order_queue[order_id]
            except Exception as e:
                print(f"Failed to produce completion event: {order_completed}")
                print(f"Reason: {e}")
        
        # Random sleep to emulate real traffic (0.5–2 seconds)
        time.sleep(random.uniform(1, 5))
    producer.flush()
if __name__ == "__main__":
    main()