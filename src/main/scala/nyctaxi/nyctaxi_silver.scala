package com.databricks.pipelines.examples.nyctaxi

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.pipelines._

/** Creates the NYC Taxi Dataset Silver Tables. */
class silverPipeline extends Pipeline with Implicits {

  // Yellow Silver
  createTable("nyc_taxi_yellow_silver")
    .query {
      val yellow_bronze = input("nyc_taxi_yellow_bronze")    
      val taxi_lookup = input("taxi_lookup")
      val taxi_rate_code = input("taxi_rate_code")
      val taxi_payment_type = input("taxi_payment_type")     
      yellow_bronze
        .join(taxi_lookup, (yellow_bronze("PULocationID") === taxi_lookup("LocationID")))
        .join(taxi_rate_code, (yellow_bronze("RatecodeID") === taxi_rate_code("RateCodeID")))
        .join(taxi_payment_type, (yellow_bronze("payment_type") === taxi_payment_type("payment_type")))
        .select("pep_pickup_date_txt", "pep_pickup_ts", "pep_dropoff_ts", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "RateCodeDesc", "payment_desc", "total_amount", "Borough", "Service_Zone")
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")    
    .expect("non zero passenger count", "passenger_count > 0")

  // Green Silver
  createTable("nyc_taxi_green_silver")  
    .query {
      val green_bronze = input("nyc_taxi_green_bronze")    
      val taxi_lookup = input("taxi_lookup")
      val taxi_rate_code = input("taxi_rate_code")
      val taxi_payment_type = input("taxi_payment_type")
      green_bronze
        .join(taxi_lookup, (green_bronze("PULocationID") === taxi_lookup("LocationID")))
        .join(taxi_rate_code, (green_bronze("RatecodeID") === taxi_rate_code("RateCodeID")))
        .join(taxi_payment_type, (green_bronze("payment_type") === taxi_payment_type("payment_type")))
        .select("pep_pickup_date_txt", "pep_pickup_ts", "pep_dropoff_ts", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "RateCodeDesc", "payment_desc", "total_amount", "Borough", "Service_Zone")
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")
    .expect("non zero passenger count", "passenger_count > 0")

}
