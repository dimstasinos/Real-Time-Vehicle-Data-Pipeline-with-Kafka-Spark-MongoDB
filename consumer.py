import pandas as pd
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer("vehicle_positions",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         bootstrap_servers='localhost:9092')

data_list = []

for message in consumer:
    car_data = message.value
    print(car_data)
    if car_data.get('end'):
        # Convert the list to a DataFrame
        if data_list:
            df = pd.DataFrame(data_list)
            print(df)
        break

        # Add the message to the list
    data_list.append(car_data)

