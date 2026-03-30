from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ["Chair", "Table", "Sofa", "Bed"]
categories = ["Furniture", "Office", "Home"]

while True:
    data = {
        "product": random.choice(products),
        "category": random.choice(categories),
        "price": round(random.uniform(50, 500), 2),
        "quantity": random.randint(1, 5),
        "timestamp": int(time.time())
    }

    producer.send("sales_topic", value=data)
    print(f"Sent: {data}")
    time.sleep(1)