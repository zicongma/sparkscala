package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain.{HeroInfo, TeamLevelInfo, TeamLevelState}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.collection.mutable.ListBuffer

class Aggregation {

  def DamageWindowSum(combatdf: DataFrame, spark: SparkSession): Unit = {

    import spark.implicits._

    val isHero = udf((name: String) => name.startsWith("npc_dota_hero"))

    val query = combatdf
      .filter($"combatType" === "damage")
      .filter(isHero('attacker))
      .withWatermark("eventTime", "2 minutes")
      .groupBy(
        window('eventTime, "5 minutes", "1 minute"),
      'attacker)
      .agg(sum('value) as 'damageTotal)
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }

  def DamageOverTime(combatdf: DataFrame, spark: SparkSession): Unit = {

    import spark.implicits._

    val isHero = udf((name: String) => name.startsWith("npc_dota_hero"))

    val query = combatdf
      .filter($"combatType" === "damage")
      .filter(isHero('attacker))
      .withWatermark("eventTime", "2 minutes")
      .groupBy(
        window('eventTime, "1 hour", "1 hour"),
        'attacker)
      .agg(sum('value) as 'damageTotal)
      .writeStream
      .outputMode("update")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }

}