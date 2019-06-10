package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain.HeroInfo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Selection {

  // This is a simple selection query for all information about team radiant with about 50% selectivity
  def TeamRadiantSelection(heroInfos: Dataset[HeroInfo]): (StreamingQuery, StructType) = {
    val query = heroInfos
      .filter(_.teamNumber == 2)
      .select("game", "name", "strength", "agility", "intellect", "eventTime")
      .select(to_json(struct("*")) as 'value)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "output")
      .option("checkpointLocation", s"/tmp/${java.util.UUID.randomUUID()}")
      .outputMode("append")
      .start()

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

  def DamageEventSelection(combatdf : DataFrame, spark: SparkSession): (StreamingQuery, StructType) = {
    import spark.implicits._

    val query = combatdf
      .filter($"combatType" === "damage")
      .writeStream
      .outputMode("update")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    val outputSchema = new StructType{}
      .add("game", IntegerType)
      .add("combatType", StringType)
      .add("attacker", StringType)
      .add("target", StringType)
      .add("value", StringType)
      .add("eventTime", StringType)

    (query, outputSchema)
  }
}

