# Real-Time Vehicle Data Pipeline with Kafka, Spark & MongoDB

This project simulates a real-time data streaming and processing system for urban vehicle movement. Using **Kafka** as a message broker, **Apache Spark** for stream processing, and **MongoDB** for scalable storage, the system ingests, transforms, and stores vehicle telemetry data generated from the UXSIM traffic simulator.

The project was developed as part of the Big Data Systems course

---

## Architecture

- **Producer**: Sends vehicle telemetry data in JSON format to Kafka topics
- **Kafka**: Handles real-time event streaming using a  topic
- **Spark Structured Streaming**: Consumes Kafka data, processes it in micro-batches
- **MongoDB**: Stores raw and processed data in a database
- **MongoDB Aggregation**: Performs analytics and insights through aggregation pipelines

---

## Technologies Used

- Kafka 3.7.0 (Scala 2.13)
- Apache Spark 3.5.1 with Kafka & MongoDB connectors
- MongoDB 7.0.11 (via MongoDB Compass)
- Python 3.x
- UXSIM traffic simulator

---

## Components

### Kafka Producer (kafkaProducer.py)
- Sends simulation data in 5-second time windows
- Adds timestamps and filters invalid entries
- Publishes each vehicle position as a separate JSON message every 4 seconds

### Kafka Consumer + Spark (spark.py)
- Reads data from Kafka topic
- Extracts and transforms JSON data
- Calculates simulation-relative timestamps
- Outputs to MongoDB:  
  - `Cars_raw`: Raw telemetry  
  - `Cars_processed`: Aggregated metrics per road segment

### MongoDB Queries (mongodbQuery.py)
- Find road segment with fewest vehicles during time range
- Find segment with highest average speed
- Identify vehicle with longest traveled distance
