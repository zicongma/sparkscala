package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain
import com.zm1216.sparkscala.SparkMain._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ListBuffer

class Stateful {

  // output the total level for 5 players for each team
  // using flatmapgroupwithstate to keep track of the most recent player level
  def TeamInfoAggregation(heroInfos: Dataset[HeroInfo], spark: SparkSession): (StreamingQuery, StructType) = {

    import spark.implicits._

    val teamInfos = heroInfos
      .groupByKey(info => (info.game, info.teamNumber))
      .flatMapGroupsWithState[TeamLevelState, TeamLevelInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (key: (Int, Int), infos: Iterator[HeroInfo], state: GroupState[TeamLevelState]) => {
        var newState =  if (state.exists) {
          state.get.stateMap
        } else {
          Map[String, Int]()
        }

        var updates = ListBuffer[TeamLevelInfo]()
        infos.foreach { info =>
          val previousVal = newState.getOrElse(info.name, -1)
          newState += (info.name -> info.level)
          val currentSum = newState.foldLeft(0)(_+_._2)
          if (previousVal != info.level) {
            updates += TeamLevelInfo(key._1, key._2, currentSum, info.eventTime)
          }
        }
        state.update(TeamLevelState(newState))
        updates.toList.toIterator
      }
    }
    val query = teamInfos
      .select('teamNumber, 'totalLevel, 'lastUpdate)
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    val outputSchema = new StructType{}
      .add("game", IntegerType)
      .add("teamNumber", IntegerType)
      .add("totalLevel", IntegerType)
      .add("lastUpdate", StringType)

    (query, outputSchema)
  }

  def PrimaryAttribute(heroInfos: Dataset[HeroInfo], spark: SparkSession): Unit = {

    import spark.implicits._

    val attributeInfos = heroInfos
      .groupByKey(_.name)
      .flatMapGroupsWithState[PrimaryAttributeState, PrimaryAttributeInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (hero: String, infos: Iterator[HeroInfo], state: GroupState[PrimaryAttributeState]) => {
        var newState =  if (state.exists) {
          state.get
        } else {
          PrimaryAttributeState("", 0)
        }
        var updates = ListBuffer[PrimaryAttributeInfo]()
        infos.foreach { info =>
          var updated = false
          if (info.strength > newState.value) {
            newState = PrimaryAttributeState("strength", info.strength)
            updated = true
          } else if (info.agility > newState.value) {
            newState = PrimaryAttributeState("agility", info.agility)
            updated = true
          } else if (info.intellect > newState.value) {
            newState = PrimaryAttributeState("intellect", info.intellect)
            updated = true
          }
          if (updated) {
            updates += PrimaryAttributeInfo(hero, newState.attribute, newState.value)
          }
        }
        state.update(PrimaryAttributeState(newState.attribute, newState.value))
        updates.toList.toIterator
      }
    }
    val query = attributeInfos
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()
    query.awaitTermination()
  }

}
