package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain.{HeroInfo, TeamLevelInfo, TeamLevelState}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.collection.mutable.ListBuffer

class Aggregation {

  // output the total level for 5 players for each team
  // using flatmapgroupwithstate to keep track of the most recent player level
  def TeamInfoAggregation(heroInfos: Dataset[HeroInfo], spark: SparkSession): Unit = {

    import spark.implicits._

    val teamInfos = heroInfos
      .groupByKey(_.teamNumber)
      .flatMapGroupsWithState[TeamLevelState, TeamLevelInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (teamNumber: Int, infos: Iterator[HeroInfo], state: GroupState[TeamLevelState]) => {
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
            updates += TeamLevelInfo(teamNumber, currentSum)
          }
        }
        state.update(TeamLevelState(newState))
        updates.toList.toIterator
      }
    }
    val query = teamInfos
//        .groupBy("id")
//      .agg(max("level") as "topLevel")
//      .groupBy("teamNumber")
//      .agg(sum("topLevel"))
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()
    query.awaitTermination()

  }

}