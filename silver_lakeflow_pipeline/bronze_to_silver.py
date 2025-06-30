import dlt
from pyspark.sql.functions import col, to_date,current_timestamp,from_utc_timestamp


@dlt.table(
    name="stage_silver"
)
def stage_silver():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/booking/data/")
    return df

@dlt.view(
    name="silver_view"
)
def silver_view():
    df = spark.readStream.table("stage_silver")
    df = df.withColumn("amount", col("amount").cast("double")) \
        .withColumn("booking_date", to_date(col("booking_date"))) \
        .withColumn("processed_time",from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
        .drop("_rescued_data")
    return df

@dlt.table(
    name="silver_booking"
)
def silver_booking():
    df = spark.readStream.table("silver_view")
    return df

################################################################################
# flights
@dlt.view(
    name="flights_view"
)
def flights_view():
  df=spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
  df=df.withColumn("flight_date",to_date(col("flight_date"))) \
    .withColumn("processed_time",from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
    .drop("_rescued_data")
  return df

@dlt.table(
  name="silver_flight"
)
def silver_flight():
  return spark.readStream.table("flights_view")

###################################################################################
#airports

@dlt.view(
    name="airports_view"
)
def airports_view():
    df=spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df=df.withColumn("processed_time",from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
    .drop("_rescued_data")
    return df


@dlt.table(
  name="silver_airport"
)
def silver_airport():
  return spark.readStream.table("airports_view")


###############################################################################################
#passengers
@dlt.view(
    name="passengers_view"
)
def passengers_view():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/passengers/data/")
    df = df.withColumn("processed_time", from_utc_timestamp(current_timestamp(), "Asia/Kolkata")) \
       .drop("_rescued_data")
    return df


@dlt.table(
  name="silver_passenger"
)
def silver_passenger():
  return spark.readStream.table("passengers_view")  

###################################################################
#joined

@dlt.table(
  name="joined_table"
)

def joined_table():
  df = spark.read.table("silver_booking") \
    .join(spark.read.table("silver_flight"), ["flight_id"]) \
    .join(spark.read.table("silver_airport"),["airport_id"]) \
    .join(spark.read.table("silver_passenger"), ["passenger_id"]) \
    .drop("processed_time")
  return df