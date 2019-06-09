package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain.HeroInfo
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class Selection {

  // This is a simple selection query for all information about team radiant with about 50% selectivity
  def TeamRadiantSelection(heroInfos: Dataset[HeroInfo]): (StreamingQuery, StructType) = {
    val query = heroInfos
      .filter(_.teamNumber == 2)
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()
    query

    val outputSchema = new StructType{}
      .add("game", IntegerType)
      .add("name", StringType)
      .add("strength", FloatType)
      .add("agility", FloatType)
      .add("intellect", FloatType)
      .add("eventTime", StringType)

    (query, outputSchema)
  }

  // This is a simple selection query for all information about hero under player id 0, about 10% selectivity
  def playerSelection(heroInfos: Dataset[HeroInfo]): Unit = {
    val query = heroInfos
      .filter(_.id == 0)
      .writeStream
      .outputMode("update")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()
    query.awaitTermination()
  }
}

