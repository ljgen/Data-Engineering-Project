import random
import time
import json
from kafka import KafkaProducer


def generate_iot_data():
    # Kafka Configuration
    KAFKA_BROKER = 'localhost:9092'
    KAFKA_TOPIC = 'iot_data'

    # Kafka Producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))    

    count = 0
    devices = ['device_1', 'device_2', 'device_3', 'device_4', 'device_5']

    try:        
        while True:
            data = {
                'device_id': random.choice(devices),             
                'timestamp': time.time(),
                'temperature': round(random.uniform(20.0, 30.0), 2),
                'humidity': round(random.uniform(40.0,60.0), 2)
            }
            # Send data to Kafka topic
            producer.send(KAFKA_TOPIC, data)

            count += 1
            print(f"Producing IoT data to Kafka topic '{KAFKA_TOPIC}' : {count} {data}")
            time.sleep(5)
    
    except KeyboardInterrupt:
        print("Interrupted! Closing producer.")
    finally:
        producer.flush()
        producer.close()

if __name__ == '__main__':
    generate_iot_data()
