package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain.HeroInfo
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class Projection {

  // This is a simple projection query obtaining the name, level, health and xp for each hero
  def BasicAttributeProjection(heroInfos: Dataset[HeroInfo]): (StreamingQuery, StructType) = {
   val query = heroInfos
        .select("game", "name", "level", "health", "xp", "eventTime")
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
      .add("level", IntegerType)
      .add("health", IntegerType)
      .add("xp", IntegerType)
      .add("eventTime", StringType)

    (query, outputSchema)
////    query
//     val query = heroInfos
//      .select("name", "level", "health", "xp")
//      .writeStream
//      .outputMode("append")
//      .format("console")
//      .option("numRows", 100)
//      .option("truncate", "false")
//      .start()
//      query


  }

}
