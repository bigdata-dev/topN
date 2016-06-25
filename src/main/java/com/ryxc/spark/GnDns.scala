package com.ryxc.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by goosoog on 15/6/10.
 * 对某应用类型中app下载量进行排名
 */

object GnApp{
  def main(args: Array[String]) {
    //inputpath as 2015061001
    //inputpath as 2015061001
    if(args.length != 3){
      System.err.println("usage: <flag> <inputpath> <outputpath>")
      System.exit(1)
    }

    // 应用类型
    val flag = args(0)

    //获取上下文对象
    val sc = new SparkContext

    //从hdfs读取数据
    val data = sc.textFile(args(1))
    val cache = data.cache()

    //排序
    cache.filter(_.split('\t').length == 4).filter(_.split('\t')(0) == flag).map((_.split('\t')(1) -> 1)).
      reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(y => (y._2, y._1)).saveAsTextFile(args(2))
  }
}
