package com.databricks.pipelines.examples.wiki

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.pipelines._

/** Loads the Wikipedia Dataset into a table. */
class WikiPipeline extends Pipeline with Implicits {
  val schema = new StructType()
    .add("id", LongType)
    .add("title", StringType)
    .add("timestamp", StringType)
    .add("contents", StringType)

  createView("raw_wiki_data")
    .query {
      spark.readStream
        .format("csv")
        .schema(schema)
        .option("delimiter", "\t")
        .option("maxFilesPerTrigger", "1")  // Let data slowly trickle in.
        .load("/databricks-datasets/wiki/") // TODO: Talk to denny... want a real dataset that is also publicly available.
    }
    .expect("valid id",    "id IS NOT NULL")
    .expect("valid title", "title IS NOT NULL")

  createTable("wiki_data")
    .query {
      readStream("raw_wiki_data")
        .withColumn("timestamp", $"timestamp".cast("timestamp"))
        .withColumn("date", $"timestamp".cast("date"))
    }
    .partitionBy("date")
    .tableProperty("pipelines.autoOptimize.zOrderCols", "timestamp")
    .expectOrFail("valid timestamp", "timestamp IS NOT NULL")

  createTable("count_per_hour")
    .query {
      read("wiki_data")
        .withColumn("window", window($"timestamp", "1 HOUR").getField("start"))
        .groupBy($"window")
        .agg(count("*") as 'count)
    }
    .expect("non zero count", "count > 0")
}