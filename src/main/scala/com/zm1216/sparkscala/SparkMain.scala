package com.zm1216.sparkscala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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

  def sessionQuery(data: DataFrame): DataFrame = {

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

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder.
      appName("SparkRateTest").
      master("local").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    var heroStream = generateHeroStream(spark)
    var resourceStream = generateResourceStream(spark)

    var query = lvlleaderboard(heroStream, spark)
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 100)
      .start()

    query.awaitTermination()
  }
}
