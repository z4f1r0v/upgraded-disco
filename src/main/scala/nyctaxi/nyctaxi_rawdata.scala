package com.databricks.pipelines.examples.nyctaxi

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.pipelines._

/** Loads the NYC Taxi Dataset into a table. */
class NYCTaxiPipeline extends Pipeline with Implicits {
  val schema_yellow = new StructType()
    .add("VendorID", IntegerType)
    .add("tpep_pickup_datetime", StringType)
    .add("tpep_dropoff_datetime", StringType)
    .add("passenger_count", IntegerType)
    .add("trip_distance", DoubleType)
    .add("RatecodeID", IntegerType)
    .add("store_and_fwd_flag", StringType)
    .add("PULocationID", IntegerType)
    .add("DOLocationID", IntegerType)
    .add("payment_type", IntegerType)
    .add("fare_amount", DoubleType)
    .add("extra", DoubleType)
    .add("mta_tax", DoubleType)
    .add("tip_amount", DoubleType)
    .add("tolls_amount", DoubleType)
    .add("improvement_surcharge", DoubleType)
    .add("total_amount", DoubleType)
    .add("congestion_surcharge", DoubleType)

  val schema_green = new StructType()
    .add("VendorID", IntegerType)
    .add("lpep_pickup_datetime", StringType)
    .add("lpep_dropoff_datetime", StringType)
    .add("store_and_fwd_flag", StringType)
    .add("RatecodeID", IntegerType)
    .add("PULocationID", IntegerType)
    .add("DOLocationID", IntegerType)
    .add("passenger_count", IntegerType)
    .add("trip_distance", DoubleType)
    .add("fare_amount", DoubleType)
    .add("extra", DoubleType)
    .add("mta_tax", DoubleType)
    .add("tip_amount", DoubleType)
    .add("tolls_amount", DoubleType)
    .add("ehail_fee", StringType)
    .add("improvement_surcharge", DoubleType)
    .add("total_amount", DoubleType)
    .add("payment_type", IntegerType)
    .add("trip_type", IntegerType)
    .add("congestion_surcharge", DoubleType)

  val schema_taxi_lookup = new StructType()
    .add("LocationID", IntegerType)
    .add("Borough", StringType)
    .add("Zone", StringType)
    .add("service_zone", StringType)

  val schema_taxi_rate_code = new StructType()
    .add("RateCodeID", IntegerType)
    .add("RateCodeDesc", StringType)

  val schema_taxi_payment_type = new StructType()
    .add("payment_type", IntegerType)
    .add("payment_desc", StringType)

  //
  // Lookup Tables
  //

  // Taxi Lookup
  createView("taxi_lookup")
    .query{
      spark.read.format("csv").schema(schema_taxi_lookup).option("delimiter", ",").option("header", "true").load("/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv")
    }

  // Taxi Rate Code
  createView("taxi_rate_code")
    .query{
      spark.read.format("csv").schema(schema_taxi_rate_code).option("delimiter", ",").option("header", "true").load("/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")
    }

  // Taxi Payment Type
  createView("taxi_payment_type")
    .query{
      spark.read.format("csv").schema(schema_taxi_payment_type).option("delimiter", ",").option("header", "true").load("/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")
    }

  //
  // Source Views
  //

  // Yellow Cab Raw Data View
  createView("raw_nyc_taxi_yellow_data")
    .query {
        spark.readStream
          .format("csv")
          .schema(schema_yellow)
          .option("delimiter", ",")
          .option("header", "true")
          .option("maxFilesPerTrigger", "1")  // Let data slowly trickle in
          .load("/databricks-datasets/nyctaxi/tripdata/yellow/*.csv.gz") // Yellow Cab, 2019-01 only
    }
    .expect("valid tpep_pickup_datetime", "tpep_pickup_datetime IS NOT NULL")   // Expect this to kick in due to many values being NULL in earlier files (e.g. 2015 datasets)
    .expect("valid tpep_dropoff_datetime", "tpep_dropoff_datetime IS NOT NULL")


  // Green Cab Raw Data View
 createView("raw_nyc_taxi_green_data")
   .query{
     spark.readStream
       .format("csv")
       .schema(schema_green)
       .option("delimiter", ",")
       .option("header", "true")
       .option("maxFilesPerTrigger", "1")
       .load("/databricks-datasets/nyctaxi/tripdata/green/*.csv.gz") // Green Cab, 2019-01 only
   }
   .expect("valid pep_pickup_datetime", "lpep_pickup_datetime IS NOT NULL")   // Expect this to kick in due to many values being NULL in earlier files (e.g. 2015 datasets)
   .expect("valid pep_dropoff_datetime", "lpep_dropoff_datetime IS NOT NULL")


}
