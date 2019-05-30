package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain.HeroInfo
import org.apache.spark.sql.{DataFrame, Dataset}

class Projection {

  // This is a simple projection query obtaining the name, level, health and xp for each hero
  def BasicAttributeProjection(heroInfos: Dataset[HeroInfo]): Unit = {
   val query = heroInfos
        .select("name", "level", "health", "xp")
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()
    query.awaitTermination()
  }

}
