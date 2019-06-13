package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain._
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

  def HPDMG(combatdf: DataFrame, heroInfos: Dataset[HeroInfo], spark: SparkSession): Unit = {

    import spark.implicits._

    val hpChange = heroInfos
      .groupByKey(info => (info.game, info.name))
      .flatMapGroupsWithState[PlayerHealthState, PlayerHealthEvent](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (key: (Int, String), infos: Iterator[HeroInfo], state: GroupState[PlayerHealthState]) => {
        var currHP =  if (state.exists) {
          state.get.hp
        } else {
          -1
        }

        var updates = ListBuffer[PlayerHealthEvent]()
        infos.foreach { info =>
          if (info.health != currHP) {
            updates += PlayerHealthEvent(key._1, key._2, info.health - currHP, info.eventTime)
          }
          currHP = info.health
        }
        state.update(PlayerHealthState(currHP))
        updates.toList.toIterator
      }
    }

    val hpChangeWatermark = hpChange.withWatermark("eventTime", "2 minutes")
    val combatDFWatermark = combatdf.withWatermark("timestamp", "10 seconds")


    hpChangeWatermark.join(
      combatDFWatermark,
      expr("""
        attacker = name AND
        timestamp >= eventTime AND
        timestamp <= eventTime + interval 5 minutes
        """)
    )
      .writeStream
      .outputMode("update")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()


  }

}