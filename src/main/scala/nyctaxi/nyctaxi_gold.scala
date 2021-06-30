package com.databricks.pipelines.examples.nyctaxi

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.pipelines._

/** Create the NYC Taxi Dataset Gold Tables. */
class goldPipeline extends Pipeline with Implicits {

  // Airports Gold
  createTable("airports")  
    .query {
      val yellow_silver = input("nyc_taxi_yellow_silver")
      val green_silver = input("nyc_taxi_green_silver")
      yellow_silver.union(green_silver).where($"service_zone" === "Airports")
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")
    .tableProperty("pipelines.metastore.tableName", "airports")

  // Brooklyn Gold
  createTable("brooklyn")  
    .query {
      val yellow_silver = input("nyc_taxi_yellow_silver")
      val green_silver = input("nyc_taxi_green_silver")
      yellow_silver.union(green_silver).where($"Borough" === "Brooklyn")
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")
    .tableProperty("pipelines.metastore.tableName", "brooklyn")

  // Manhattan Gold
  createTable("manhattan")  
    .query {
      val yellow_silver = input("nyc_taxi_yellow_silver")
      val green_silver = input("nyc_taxi_green_silver")
      yellow_silver.union(green_silver).where($"Borough" === "Manhattan")
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")
    .tableProperty("pipelines.metastore.tableName", "manhattan")

  // Bronx Gold
  createTable("bronx")  
    .query {
      val yellow_silver = input("nyc_taxi_yellow_silver")
      val green_silver = input("nyc_taxi_green_silver")
      yellow_silver.union(green_silver).where($"Borough" === "Bronx")
    }
    .partitionBy("pep_pickup_date_txt")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "pep_pickup_date_txt")
    .tableProperty("pipelines.metastore.tableName", "bronx")
}
