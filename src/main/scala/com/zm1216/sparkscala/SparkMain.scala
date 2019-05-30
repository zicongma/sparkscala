package com.zm1216.sparkscala

import java.sql.Timestamp

import com.zm1216.sparkscala.queries.{Aggregation, MathCalculation, Projection}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ListBuffer

/**
 * Hello world!
 *
 */
object SparkMain{

  case class TeamPlayerPositions(stateMap: Map[String, (Double, Double)])

  case class TeamAreaControlled(teamNumber: Int, areaControlled: Float)

  case class TeamLevelState(stateMap: Map[String, Int])

  case class TeamLevelInfo(teamNumber: Int, totalLevel: Int)

  case class Message(action: String, entity: String, metaData: String)


  case class HeroState(stateMap: Map[String, String])

  case class ResourceState(stateMap: Map[String, String])

  case class ResourceInfo(id: Int, kill: Int, assist: Int, death: Int, eventTime: String)

  case class HeroInfo(name: String, id: Int, level: Int, xp: Int, health: Int, lifeState: Int, cellX: Int, cellY: Int,
                      vecX: Float, vecY: Float, teamNumber: Int, damageMin: Int, damageMax: Int,  strength: Float,
                      agility: Float, intellect: Float, eventTime: String)


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._


    val spark = SparkSession.
      builder.
      appName("SparkRateTest").
      master("local").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val herodf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "hero")
      .load
      .select('value.cast("string"))

    val resourcedf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "resource")
      .load
      .select('value.cast("string"))

    val heroMessages = herodf
      .as[String]
      .map { case line =>
        val items = line.split("/")
        Message(items(0), items(1), line)
      }

    val resourceMessages = resourcedf
      .as[String]
      .map { case line =>
        val items = line.split("/")
        Message(items(0), items(1), line)
      }

    val heroInfos = heroMessages
      .groupByKey(_.entity)
      .flatMapGroupsWithState[HeroState, HeroInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (entity: String, messages: Iterator[Message], state: GroupState[HeroState]) =>

       var newState =  if (state.exists) {
          state.get.stateMap
        } else {
         Map("m_iPlayerID" -> "", "m_iCurrentLevel" -> "", "m_iCurrentXP" -> "", "m_iHealth" -> "", "m_lifeState" -> "",
           "CBodyComponent.m_cellX" -> "", "CBodyComponent.m_cellY" -> "", "CBodyComponent.m_vecX" -> "",
           "CBodyComponent.m_vecY" -> "", "m_iTeamNum" -> "", "m_iDamageMin" -> "", "m_iDamageMax" -> "",
           "m_flStrength" -> "", "m_flAgility" -> "", "m_flIntellect" -> "", "eventTime" -> "")
       }
        var updates = new ListBuffer[HeroInfo]()
        messages.foreach { message =>
          val items = message.metaData.split("/")
          if (message.action == "initialize") {
            for (a <- 2 until items.length - 2 by 2) {
              newState += (items(a) -> items(a+1))
            }
            newState += ("eventTime" -> items(items.length - 1))
          } else {
            newState += (items(2) -> items(3))
            newState += ("eventTime" -> items(4))
          }
          updates += HeroInfo(entity, newState("m_iPlayerID").toInt, newState("m_iCurrentLevel").toInt,
            newState("m_iCurrentXP").toInt, newState("m_iHealth").toInt, newState("m_lifeState").toInt,
            newState("CBodyComponent.m_cellX").toInt, newState("CBodyComponent.m_cellY").toInt,
            newState("CBodyComponent.m_vecX").toFloat, newState("CBodyComponent.m_vecY").toFloat,
            newState("m_iTeamNum").toInt, newState("m_iDamageMin").toInt, newState("m_iDamageMax").toInt,
            newState("m_flStrength").toFloat, newState("m_flAgility").toFloat,
            newState("m_flIntellect").toFloat, newState("eventTime"))
        }
        state.update(HeroState(newState))
        updates.toList.toIterator
    }

    val resourceInfos = resourceMessages
      .groupByKey(_.entity)
      .flatMapGroupsWithState[ResourceState, ResourceInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (entity: String, messages: Iterator[Message], state: GroupState[ResourceState]) =>

        var newState =  if (state.exists) {
          state.get.stateMap
        } else {
          Map("m_iPlayerID" -> entity.charAt(entity.length - 1).toString, "m_iKills" -> "", "m_iAssists" -> "",
            "m_iDeaths" -> "", "eventTime" -> "")
        }

        var updates = new ListBuffer[ResourceInfo]()
        messages.foreach { message =>
          val items = message.metaData.split("/")
          if (message.action == "initialize") {
            for (a <- 2 until items.length - 2 by 2) {
              val strs = items(a).split('.')
              newState += (strs(2) -> items(a+1))
            }
            newState += ("eventTime" -> items(items.length - 1))
          } else {
            val strs = items(2).split('.')
            newState += (strs(2) -> items(3))
            newState += ("eventTime" -> items(4))
          }
          updates += ResourceInfo(newState("m_iPlayerID").toInt, newState("m_iKills").toInt,
            newState("m_iAssists").toInt, newState("m_iDeaths").toInt, newState("eventTime"))
        }
        state.update(ResourceState(newState))
        updates.toList.toIterator

    }

    new MathCalculation().TerritoryControled(heroInfos, spark)


    //    val getEntity = udf((value: String) => value.split("/")(0))
//    val getProperty = udf((value: String) => value.split("/")(1))
//    val getNewvalue = udf((value: String) => value.split("/")(2))
//    val getEventTime = udf((value: String) => value.split("/")(3))
//
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "update")
//      .load
//      .select(
//        getEntity('value) as 'entity,
//        getProperty('value) as 'property,
//        getNewvalue('value) as 'newValue,
//        getEventTime('value) as 'eventTime
//      )


//    var query = heroInfos
//        .join(resourceInfos,
//          Seq("id"),
//          joinType = "inner")
//        .withColumn("eventTime", col("eventTime").cast("timestamp"))
//        .withWatermark("eventTime", "2 minutes")
//        .groupBy(
//          window($"eventTime", "1 hour", "1 hour"),
//          $"id"
//        ).avg("health")
//        .writeStream
//        .outputMode("append")
//        .format("console")
//        .option("numRows", 100)
//        .option("truncate", "false")
//        .start()

//    var heroStream = generateHeroStream(spark)
//    var resourceStream = generateResourceStream(spark)
//
//    var query = lvlleaderboard(heroStream, spark)
//      .writeStream
//      .outputMode("update")
//      .format("console")
//      .option("truncate", "false")
//      .option("numRows", 100)
//      .start()
//
//    query.awaitTermination()
  }
}
