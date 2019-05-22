package com.zm1216.sparkscala

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._

import scala.collection.mutable

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


  case class Update(entity: String,  property: String, newValue: String, eventTime: String, timestamp: Timestamp)

  case class HeroState(heroMap: mutable.HashMap[String, HeroInfo]) {
  }


  case class HeroInfo(name: String, id: Int, xp: Int, health: Int) {
    def updateID(id: Int): HeroInfo = {
      HeroInfo(this.name, id, this.xp, this.health)
    }
    def updatexp(xp: Int): HeroInfo = {
      HeroInfo(this.name, this.id, xp, this.health)
    }
    def updateHealth(health: Int): HeroInfo = {
      HeroInfo(this.name, this.id, this.xp, health)
    }
  }

  def updateHeroMap(heroMap: mutable.HashMap[String, HeroInfo], hero: String, property: String, value: String) : mutable.HashMap[String, HeroInfo] = {
    //println(property, value)
    if (heroMap.contains(hero)) {
      property match {
        case "m_iplayerID" => heroMap += (hero -> heroMap(hero).updateID(value.toInt))
        case "m_iHealth" => heroMap += (hero -> heroMap(hero).updateHealth(value.toInt))
        case "m_iCurrentXP" => heroMap += (hero -> heroMap(hero).updatexp(value.toInt))
        case _ => heroMap
      }
    } else {
      heroMap += ((hero, HeroInfo(hero, -1, -1, -1)))
      property match {
        case "m_iplayerID" => heroMap += (hero -> heroMap(hero).updateID(value.toInt))
        case "m_iHealth" => heroMap += (hero -> heroMap(hero).updateHealth(value.toInt))
        case "m_iCurrentXP" => heroMap += (hero -> heroMap(hero).updatexp(value.toInt))
        case _ => heroMap
      }
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
      .select('value.cast("string"), 'timestamp)

    val updates = df
      .as[(String, Timestamp)]
      .map { case(line, timestamp) =>
        val items = line.split("/")
        Update(items(0), items(1), items(2), items(3), timestamp)
      }
      .filter(update => update.property == "m_iCurrentXP" || update.property == "m_iHealth")

    val infos = updates
      .groupByKey(update => update.entity)
      .mapGroupsWithState[HeroState, HeroInfo](GroupStateTimeout.NoTimeout()) {
      case (entity: String, updates: Iterator[Update], state: GroupState[HeroState]) =>
        val update = updates.next()
        //println(update.entity, update.property, update.newValue)
        val updatedState = if (state.exists) {
          println("State Exist!!!")
          val oldState = state.get
          HeroState(updateHeroMap(oldState.heroMap, update.entity, update.property, update.newValue))
        } else {
          var heroMap = new mutable.HashMap[String, HeroInfo]()
          HeroState(updateHeroMap(heroMap, update.entity, update.property, update.newValue))
        }
        state.update(updatedState)
        state.get.heroMap.get(update.entity).orNull
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
