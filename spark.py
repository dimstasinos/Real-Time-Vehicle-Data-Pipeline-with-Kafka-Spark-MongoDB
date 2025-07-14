from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize a default value to simulation_start, date_check and start_time_tmp
simulation_start = "00/00/00 00:00:00"
date_check = False
start_time_tmp = []


# Send raw_data to collection Cars_raw of Big_Data_Project at our local MongoDB database (create database and
# collection, if they don't exist)
def raw_data_db_send(raw_dataframe):

    raw_dataframe.show()

    # Athens Timezone for mongodb
    raw_dataframe_add_utc = raw_dataframe.withColumn("time", from_utc_timestamp(raw_dataframe["time"], '+3:00'))

    # Send raw data to mongodb
    raw_dataframe_add_utc.write \
        .mode("append") \
        .format("mongodb") \
        .option("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
        .option("spark.mongodb.database", "Big_Data_Project") \
        .option("spark.mongodb.collection", "Cars_raw") \
        .save()


def process_data_send_db(data, id):
    global simulation_start, date_check, start_time_tmp  # Make values global

    # Create JSON to dataframe through an SQL expression for unprocessed data
    data_dataframe_raw = data.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select("data.*") \
        .withColumn("time", to_timestamp("time", "dd/MM/yyyy HH:mm:ss"))  # Transform column value of time to timestamp

    # Add a column with the simulation start string
    data_dataframe = data_dataframe_raw.withColumn("simulation_start",
                                                   lit(simulation_start))

    if not start_time_tmp:  # If it is an empty list
        start_time_tmp = data_dataframe.select("time").where(
            ''' name=='null'  ''').collect()  # This is used only in order to get the simulation start value
        # No need to do unnecessary select queries, so we do it only once we get the value

    if range(len(start_time_tmp) != 0) and date_check is False:  # Check if we got the simulation start value
        simulation_start = start_time_tmp[0].time  # Get value we got from Select
        date_check = True
    elif date_check is True:  # Send raw and processed data , as asked
        raw_data_db_send(data_dataframe_raw)  # Send raw data with this function

        # Change time column to t, where t the difference between simulation start and time
        data_dataframe = data_dataframe.withColumn("time", (unix_timestamp(col("time")) -
                                                            unix_timestamp(
                                                                "simulation_start")))

        # Query only asked values (time,link,speed) and create columns with count of link for grouped by link,
        # time and avg of speed
        processed_dataframe = data_dataframe.selectExpr("time", "link", "speed") \
            .groupby("link", "time") \
            .agg(count("link").alias("vcount"),
                 avg("speed").alias(
                     "vspeed"))

        processed_dataframe.show()

        # Send values to collection Cars_processed of Big_Data_Project at our local MongoDB database (create database
        # and collection, if they don't exist)
        processed_dataframe.write \
            .format("mongodb") \
            .mode("append") \
            .option("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
            .option("spark.mongodb.database", "Big_Data_Project") \
            .option("spark.mongodb.collection", "Cars_processed") \
            .save()


# Create default schema for Kafka Data
schema = StructType([
    StructField("name", StringType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
    StructField("time", StringType()),
    StructField("link", StringType()),
    StructField("position", FloatType()),
    StructField("spacing", FloatType()),
    StructField("speed", FloatType())
])
# Create spark session
spark = SparkSession.builder.appName("VehicleDataSparkProcess").getOrCreate()

# Read incoming stream and insert into vehicle_positions topic
kafka_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle_positions") \
    .load()

# Once we receive the first value every t seconds, wait for 2 seconds, then create a batch and send it to MongoDB
# This is used in order to make sure that data is handled correctly in case some data come later
query = kafka_data.writeStream.trigger(processingTime="2 second").outputMode("append").foreachBatch(
    process_data_send_db).start()

query.awaitTermination()  # Await termination call
