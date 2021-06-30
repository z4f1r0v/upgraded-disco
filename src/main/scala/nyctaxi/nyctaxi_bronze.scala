package com.databricks.pipelines.examples.nyctaxi

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.pipelines._

/** Creates the NYC Taxi Dataset bronze tables. */
class bronzePipeline extends Pipeline with Implicits {
  // Yellow Cab Bronze Table
  createTable("nyc_taxi_yellow_bronze")
    .query {
      input("raw_nyc_taxi_yellow_data")
        .withColumn("pep_pickup_ts", $"tpep_pickup_datetime".cast("timestamp"))
        .withColumn("pep_dropoff_ts", $"tpep_dropoff_datetime".cast("timestamp"))
        .withColumn("pep_pickup_date_txt", expr("substring(tpep_pickup_datetime, 1, 10)"))
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "tpep_pickup_date_txt")
    .expectOrFail("valid pep_pickup_date_txt", "pep_pickup_date_txt IS NOT NULL")

  //  Green Cab Bronze Table
  createTable("nyc_taxi_green_bronze")
    .query {
      input("raw_nyc_taxi_green_data")
        .withColumn("pep_pickup_ts", $"lpep_pickup_datetime".cast("timestamp"))
        .withColumn("pep_dropoff_ts", $"lpep_dropoff_datetime".cast("timestamp"))
        .withColumn("pep_pickup_date_txt", expr("substring(lpep_pickup_datetime, 1, 10)"))
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")
    .expectOrFail("valid pep_pickup_date_txt", "pep_pickup_date_txt IS NOT NULL")

}
