package com.zm1216.sparkscala

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Hello world!
 *
 */
object SparkMain{
  def generateHeroStream(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val r = scala.util.Random.nextInt(10)
    spark.readStream.
      format("rate").
      option("rowsPerSecond", 10).
      load.
      select(('value % 10) as 'playerID,
        'value % 25 as 'level,
        'value % 27235 as 'xp,
        'value % 3000 as 'health,
        'value % 4 as 'lifeState,
        'value % 128 as 'cellX,
        'value % 128 as 'cellY,
        ('value % 1280) / 10 as 'vecX,
        ('value % 1280) / 10 as 'vecY,
        'value % 2 as 'team,
        'value % 200 as 'strength,
        'value % 200 as 'agility,
        'value % 200 as 'intellect,
        'value % 100 as 'damageMin,
        'value % 100 as 'damageMax,
        'timestamp
)
  }

  def generateResourceStream(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.readStream.
      format("rate")
      .option("rowsPerSecond", 10)
      .load
      .select('value % 10 as 'playerID,
        'value % 15 as 'kill,
      'value % 15 as 'death,
      'value % 20 as 'assist)
  }

  // simple filtering query
  def filteringQuery(data: DataFrame): DataFrame = {
    data.filter("playerID == 0")
  }

  // calculate the average xp of each player within 10 seconds window
  def averageXP(data: DataFrame, spark:SparkSession): DataFrame = {
    import spark.implicits._
    data.groupBy(
      window($"timestamp", "10 seconds", "10 seconds"),
      $"playerID"
    ).avg("xp")
  }

  // leaderboard query that calculate the level gained by players within 1 minute window
  def lvlleaderboard(data: DataFrame, spark:SparkSession): DataFrame = {
    import spark.implicits._
    data.withWatermark("timestamp", "5 seconds")
      .groupBy(
      window($"timestamp", "1 minute", "1 minute"),
      $"playerID"
    ).agg(max('level),
      min('level),
      max('level) - min('level) as 'levelGained)
  }

  def worldCoordinate(data: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    data.select(
      'cellX * 128 + 'vecX as 'worldX,
      'cellY * -128 - 'vecY + 32768 as 'worldY
    )
  }

  // kda leaderboard query that calculates the number of people killed by each player within 1 minute window
  // inner join + aggregation
  def kdaleaderboard(hero: DataFrame, resource: DataFrame, spark:SparkSession): DataFrame = {
    import spark.implicits._
    hero.join(resource,
      Seq("playerID"),
      joinType = "inner")
      .withWatermark("timestamp", "5 seconds")
      .groupBy(
        window($"timestamp", "1 minute", "1 minute"),
        $"playerID"
      )
      .agg(max('kill),
        min('kill),
        max('kill) - min('kill) as 'killed)
  }


  case class Message(action: String, entity: String, metaData: String)


  case class HeroState(id: Int, health: Int, xp: Int, eventTime: String) {
    def updateID(id: Int, eventTime: String): HeroState = {
      HeroState(id, this.health, this.xp, eventTime)
    }
    def updateHealth(health: Int, eventTime: String): HeroState = {
      HeroState(this.id, health, this.xp, eventTime)
    }
    def updatexp(xp: Int, eventTime: String): HeroState = {
      HeroState(this.id, this.health, xp, eventTime)
    }
  }


  case class HeroInfo(name: String, id: Int, health: Int, xp: Int, eventTime: String)

  def updateHeroState(oldState: HeroState, property: String, value: String, eventTime: String): HeroState = {
    property match {
      case "m_iPlayerID" => oldState.updateID(value.toInt, eventTime)
      case "m_iHealth" => oldState.updateHealth(value.toInt, eventTime)
      case "m_iCurrentXP" => oldState.updatexp(value.toInt, eventTime)
      case _ => oldState
    }
  }


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._


    val spark = SparkSession.
      builder.
      appName("SparkRateTest").
      master("local").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "update")
      .load
      .select('value.cast("string"))

    val messages = df
      .as[String]
      .map { case line =>
        val items = line.split("/")
        Message(items(0), items(1), line)
      }

    val infos = messages
      .groupByKey(_.entity)
      .flatMapGroupsWithState[HeroState, HeroInfo](OutputMode.Update(), GroupStateTimeout.NoTimeout()) {
      case (entity: String, messages: Iterator[Message], state: GroupState[HeroState]) =>

       var newState =  if (state.exists) {
          state.get
        } else {
          HeroState(-1, -1, -1, "")
       }
        var updates = new ListBuffer[HeroInfo]()
        messages.foreach { message =>
          val items = message.metaData.split("/")
          if (message.action == "initialize") {
            for (a <- 2 until items.length - 2 by 2) {
              newState = updateHeroState(newState, items(a), items(a+1), items(items.length - 1))
            }
          } else {
            newState = updateHeroState(newState, items(2), items(3), items(4))
          }
          updates += HeroInfo(entity, newState.id, newState.health, newState.xp, newState.eventTime)
        }

        state.update(newState)
        updates.toList.toIterator
    }


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


    var query = infos
        .writeStream
        .outputMode("update")
        .format("console")
        .option("numRows", 100)
        .option("truncate", "false")
        .start()

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

    query.awaitTermination()
  }
}
