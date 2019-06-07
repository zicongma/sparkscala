package com.zm1216.sparkscala

import java.sql.Timestamp

import com.zm1216.sparkscala.queries._
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

  @volatile private var numRecs: Long = 0L
  @volatile private var startTime: Long = 0L
  @volatile private var endTime: Long = 0L

  class ThroughputListener extends StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      startTime = System.currentTimeMillis()
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      numRecs += event.progress.numInputRows
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      endTime = System.currentTimeMillis()
    }
  }

  case class TeamPlayerPositions(stateMap: Map[String, (Double, Double)])

  case class TeamAreaControlled(teamNumber: Int, positions: List[(Double, Double)])

  case class PrimaryAttributeState(attribute: String, value: Float)

  case class PrimaryAttributeInfo(name: String, attribute: String, value: Float)

  case class KDAState(kill: Int, death: Int, assist: Int)

  case class KDAInfo(id: Int, value: Float)

  case class TeamLevelState(stateMap: Map[String, Int])

  case class TeamLevelInfo(game: Int, teamNumber: Int, totalLevel: Int, lastUpdate: String)

  case class Message(action: String, key: String, metaData: String)


  case class HeroState(stateMap: Map[String, String])

  case class ResourceState(stateMap: Map[String, String])

  case class ResourceInfo(game: Int, id: Int, kill: Int, assist: Int, death: Int, eventTime: String)

  case class HeroInfo(game: Int, name: String, id: Int, level: Int, xp: Int, health: Int, lifeState: Int, cellX: Int, cellY: Int,
                      vecX: Float, vecY: Float, teamNumber: Int, damageMin: Int, damageMax: Int,  strength: Float,
                      agility: Float, intellect: Float, eventTime: String)


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    val spark = SparkSession
      .builder
      .config("spark.streaming.stopGracefullyOnShutdown", true)
      .config("spark.sql.streaming.metricsEnabled",true)
      .appName("GameBench")
      .master("local")
      .getOrCreate()

    spark.streams.addListener(new ThroughputListener)

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

    val getGameNumber = udf((value: String) => value.split("/")(0))
    val getCombatType = udf((value: String) => value.split("/")(1))
    val getAttacker = udf((value: String) => value.split("/")(2))
    val getTarget = udf((value: String) => value.split("/")(3))
    val getValue = udf((value: String) => value.split("/")(4))
    val getEventTime = udf((value: String) => value.split("/")(5))

    val combatdf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "combatlog")
      .option("failOnDataLoss", "false")
      .load
      .select(getGameNumber('value) as 'game,
        getCombatType('value) as 'combatType,
        getAttacker('value) as 'attacker,
        getTarget('value) as 'target,
        getValue('value) as 'value,
        getEventTime('value).cast("timestamp") as 'eventTime)

    val heroMessages = herodf
      .as[String]
      .map { case line =>
        val items = line.split("/")
        Message(items(0), items(1) + items(2), line)
      }

    val resourceMessages = resourcedf
      .as[String]
      .map { case line =>
        val items = line.split("/")
        Message(items(0), items(1) + items(2), line)
      }

    val heroInfos = heroMessages
      .groupByKey(_.key)
      .flatMapGroupsWithState[HeroState, HeroInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (key: String, messages: Iterator[Message], state: GroupState[HeroState]) =>

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
            for (a <- 3 until items.length - 2 by 2) {
              newState += (items(a) -> items(a + 1))
            }
            newState += ("eventTime" -> items(items.length - 1))
          } else {
            newState += (items(3) -> items(4))
            newState += ("eventTime" -> items(5))
          }
          updates += HeroInfo(items(1).toInt, items(2), newState("m_iPlayerID").toInt, newState("m_iCurrentLevel").toInt,
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
      .groupByKey(_.key)
      .flatMapGroupsWithState[ResourceState, ResourceInfo](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (key: String, messages: Iterator[Message], state: GroupState[ResourceState]) =>

        var newState =  if (state.exists) {
          state.get.stateMap
        } else {
          Map("m_iPlayerID" -> key.charAt(key.length - 1).toString, "m_iKills" -> "", "m_iAssists" -> "",
            "m_iDeaths" -> "", "eventTime" -> "")
        }

        var updates = new ListBuffer[ResourceInfo]()
        messages.foreach { message =>
          val items = message.metaData.split("/")
          if (message.action == "initialize") {
            for (a <- 3 until items.length - 2 by 2) {
              val strs = items(a).split('.')
              newState += (strs(2) -> items(a+1))
            }
            newState += ("eventTime" -> items(items.length - 1))
          } else {
            println("resource")
            val strs = items(3).split("\\.")
            newState += (strs(2) -> items(4))
            newState += ("eventTime" -> items(5))
          }
          println(key)
          updates += ResourceInfo(items(1).toInt, newState("m_iPlayerID").toInt, newState("m_iKills").toInt,
            newState("m_iAssists").toInt, newState("").toInt, newState("eventTime"))
        }
        state.update(ResourceState(newState))
        updates.toList.toIterator

    }

    val (query, outputSchema) = new Projection().BasicAttributeProjection(heroInfos)
    query.awaitTermination(1320000)
    query.stop()
    val realTimeMs = udf((t: java.sql.Timestamp) => t.getTime)
    println("\n THROUGHPUT FOR MATH CALCULATION \n" + numRecs * 1000 / (endTime - startTime) + "\n")
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "output")
      .load()
      .withColumn("result", from_json('value.cast("string"), outputSchema))
      .select(col("result.eventTime").cast("timestamp") as "eventTime", 'timestamp)
      .select(realTimeMs('timestamp) - realTimeMs('eventTime) as 'diff)
      .selectExpr(
        "min(diff) as latency_min",
        "mean(diff) as latency_avg",
        "percentile_approx(diff, 0.95) as latency_95",
        "percentile_approx(diff, 0.99) as latency_99",
        "max(diff) as latency_max")
      .show()


//    val query = new MathCalculation().TerritoryControled(heroInfos, spark)
//    query.awaitTermination()
  }
}
