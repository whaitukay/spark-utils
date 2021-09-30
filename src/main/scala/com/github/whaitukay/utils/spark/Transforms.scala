package com.github.whaitukay.utils.spark

import org.apache.spark.sql.Row

object Transforms {

  import scala.annotation.tailrec
  /**
   * Join a Seq of DataFrames using a tree structure instead of staggered.
   * This is useful when joining many dataframes togerther on the same key.
   *
   * Usage:
   *  val mySeqOfDF = Seq( df1, df2, df3, ... )
   *  val myJoinedDF = binaryJoin( mySeqOfDF, "consumerid", "left")
   *
   *  @param arr: Sequence of Dataframes to join on similar key
   *  @param key: Key to join on
   *  @param joinType: Type of join to perform (default: left)
   */
  @tailrec
  final def binaryJoin(arr: Seq[org.apache.spark.sql.Dataset[Row]], key: String = "aggrkey", joinType: String = "left"): org.apache.spark.sql.Dataset[Row] = {
    if(arr.size == 1){
      arr.head
    } else {
      val intermediateResult = if(arr.size % 2 == 0){ //If even:
        arr.grouped(2).toArray.map{ s =>
          s.head.join(s(1), Seq(key), joinType)
        }
      } else {                                        //else odd:
        arr.dropRight(1).grouped(2).toArray.map{ s =>
          s.head.join(s(1), Seq(key), joinType)
        } ++ arr.takeRight(1)
      }
      binaryJoin(intermediateResult, key, joinType)
    }
  }
}
