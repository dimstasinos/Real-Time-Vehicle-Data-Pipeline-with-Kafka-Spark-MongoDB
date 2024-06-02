import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from sympy import false
from uxsim import *

seed = None

W = World(
    name="",
    deltan=5,
    tmax=120,  # 1 hour simulation
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)
random.seed(seed)

# network definition
"""
    N1  N2  N3  N4 
    |   |   |   |
W1--I1--I2--I3--I4-<E1
    |   |   |   |
    v   ^   v   ^
    S1  S2  S3  S4
"""

signal_time = 20
sf_1 = 1
sf_2 = 1

I1 = W.addNode("I1", 1, 0, signal=[signal_time * sf_1, signal_time * sf_2])
I2 = W.addNode("I2", 2, 0, signal=[signal_time * sf_1, signal_time * sf_2])
I3 = W.addNode("I3", 3, 0, signal=[signal_time * sf_1, signal_time * sf_2])
I4 = W.addNode("I4", 4, 0, signal=[signal_time * sf_1, signal_time * sf_2])
W1 = W.addNode("W1", 0, 0)
E1 = W.addNode("E1", 5, 0)
N1 = W.addNode("N1", 1, 1)
N2 = W.addNode("N2", 2, 1)
N3 = W.addNode("N3", 3, 1)
N4 = W.addNode("N4", 4, 1)
S1 = W.addNode("S1", 1, -1)
S2 = W.addNode("S2", 2, -1)
S3 = W.addNode("S3", 3, -1)
S4 = W.addNode("S4", 4, -1)

# E <-> W direction: signal group 0
for n1, n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
    W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3,
              signal_group=0)

# N -> S direction: signal group 1
for n1, n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
    W.addLink(n1.name + n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

# S -> N direction: signal group 2
for n1, n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
    W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

# random demand definition every 30 seconds
dt = 30
demand = 2  # average demand for the simulation time
demands = []
for t in range(0, 3600, dt):
    dem = random.uniform(0, demand)
    for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
        W.adddemand(n1, n2, t, t + dt, dem * 0.25)
        demands.append({"start": n1.name, "dest": n2.name, "times": {"start": t, "end": t + dt}, "demand": dem})
    for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1], [S4, W1]]:
        W.adddemand(n1, n2, t, t + dt, dem * 0.75)
        demands.append({"start": n1.name, "dest": n2.name, "times": {"start": t, "end": t + dt}, "demand": dem})

W.exec_simulation()
cars = W.analyzer.vehicles_to_pandas()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

i = 0
start_time = datetime.now()
start_time.strftime('%Y/%m/%d %H:%M:%S')
while i <= max(cars["t"]):
    cars_data_t = cars[cars.t == i]
    i = i + 5
    if not cars_data_t.empty:
        json_cars = cars_data_t.to_json(orient='records')
        cars_data = json.loads(json_cars)
        for car in cars_data:
            value = start_time + timedelta(seconds=car["t"])
            car["t"] = value.strftime('%d/%m/%Y %H:%M:%S')
            if car["link"] != "waiting_at_origin_node":
                producer.send('vehicle_positions', car)


producer.send('vehicle_positions', {'end': True})
producer.flush()
producer.close()
