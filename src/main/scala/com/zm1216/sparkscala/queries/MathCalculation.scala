package com.zm1216.sparkscala.queries

import com.zm1216.sparkscala.SparkMain._
import org.apache.spark.sql.functions.{struct, to_json, udf}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

class MathCalculation {

  // simple calculation query to calculate hero's position based on vec and cell values
  def HeroPositionCalculation(heroInfos: Dataset[HeroInfo], spark: SparkSession): (StreamingQuery, StructType) = {

    import spark.implicits._

    val query = heroInfos
      .select('game,
        'name,
        'cellX * 128 + 'vecX as 'worldX,
        'cellY * -128 - 'vecY + 32768 as 'worldY,
        'eventTime
      )
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    val outputSchema = new StructType{}
      .add("game", IntegerType)
      .add("name", StringType)
      .add("worldX", FloatType)
      .add("worldY", FloatType)
      .add("eventTime", StringType)

    (query, outputSchema)
  }



  def TerritoryControled(heroInfos: Dataset[HeroInfo], spark: SparkSession): StreamingQuery = {

    import spark.implicits._

    val areaInfos = heroInfos
      .groupByKey(_.teamNumber)
      .flatMapGroupsWithState[TeamPlayerPositions, TeamAreaControlled](OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
      case (teamNumber: Int, infos: Iterator[HeroInfo], state: GroupState[TeamPlayerPositions]) => {
        var newState =  if (state.exists) {
          state.get.stateMap
        } else {
          Map[String, (Double, Double)]()
        }

        var updates = ListBuffer[TeamAreaControlled]()
        infos.foreach { info =>
          val previousVal = newState.getOrElse(info.name, -1)
          newState += (info.name -> (info.cellX * 128 + info.vecX, info.cellY * -128 - info.vecY + 32768))
          if (newState.size == 5) {
            val points = newState.values.toList
            var sortedPoints = points.sortWith(m(_,(0.0,0.0)) < m(_,(0.0,0.0)))
            updates += TeamAreaControlled(teamNumber, points)
          }
        }
        state.update(TeamPlayerPositions(newState))
        updates.toList.toIterator
      }
    }

    val query = areaInfos
      .writeStream
      .outputMode("append")
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()

    query
  }

  def calculateAreaUnderSegment(A: (Double, Double) , B: (Double, Double)): Double = {
    val averHeight = (A._2 + B._2)/2
    val width = A._1 - B._1
    width * averHeight
  }

  def get_hull(points:List[(Double,Double)], hull:List[(Double,Double)]):List[(Double,Double)] = points match{
    case Nil  =>            join_tail(hull,hull.size -1)
    case head :: tail =>    get_hull(tail,reduce(head::hull))
  }
  def reduce(hull:List[(Double,Double)]):List[(Double,Double)] = hull match{
    case p1::p2::p3::rest => {
      if(check_point(p1,p2,p3))      hull
      else                           reduce(p1::p3::rest)
    }
    case _ =>                          hull
  }
  def check_point(pnt:(Double,Double), p2:(Double,Double),p1:(Double,Double)): Boolean = {
    val (x,y) = (pnt._1,pnt._2)
    val (x1,y1) = (p1._1,p1._2)
    val (x2,y2) = (p2._1,p2._2)
    ((x-x1)*(y2-y1) - (x2-x1)*(y-y1)) <= 0
  }
  def m(p1:(Double,Double), p2:(Double,Double)):Double = {
    if(p2._1 == p1._1 && p1._2>p2._2)       90
    else if(p2._1 == p1._1 && p1._2<p2._2)  -90
    else if(p1._1<p2._1)                    180 - Math.toDegrees(Math.atan(-(p1._2 - p2._2)/(p1._1 - p2._1)))
    else                                    Math.toDegrees(Math.atan((p1._2 - p2._2)/(p1._1 - p2._1)))
  }
  def join_tail(hull:List[(Double,Double)],len:Int):List[(Double,Double)] = {
    if(m(hull(len),hull(0)) > m(hull(len-1),hull(0)))   join_tail(hull.slice(0,len),len-1)
    else                                                hull
  }



}
