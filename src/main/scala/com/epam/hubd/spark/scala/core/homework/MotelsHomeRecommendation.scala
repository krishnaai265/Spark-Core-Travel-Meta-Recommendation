package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = sc.textFile(bidsPath)
    .map( x => x.split(",").toList)

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = rawBids
    .filter(row => row(2).startsWith("ERROR_"))
    .map(row => (BidError(row(1), row(2)), 1))
    .reduceByKey(_+_)
    .map(row => row._1.toString +","+row._2.toString)

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = sc.textFile(exchangeRatesPath)
    .map(row => (row.split(",")(0), row.split(",")(3).toDouble))
    .collect().toMap

  def setDateFormat (item: BidItem): BidItem = {
    val time: DateTime = Constants.INPUT_DATE_FORMAT.parseDateTime(item.bidDate)
    return new BidItem(item.motelId, Constants.OUTPUT_DATE_FORMAT.print(time), item.loSa, item.price)
  }

  def calculateEURPrice (item: BidItem, exchangeRates: Map[String, Double]): BidItem = {
    val exRate: Double = exchangeRates.getOrElse(item.bidDate, 0.87)
    return new BidItem(item.motelId, item.bidDate, item.loSa,  BigDecimal(item.price * exRate).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
  }

  def getMaxBidItem(list: List[BidItem]) = {
    var maxPrice = 0.0
    var result = list(0)
    list.foreach(row => if (row.price > maxPrice) {
      result = row
      maxPrice = row.price
    })
    result
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val correctBidsMap = rawBids.filter(row => !row(2).startsWith("ERROR_"))
    correctBidsMap.flatMap((row: List[String]) => List(
      (row(0), row(1), "US", row(5)),
      (row(0), row(1), "CA", row(8)),
      (row(0), row(1), "MX", row(6))))
      .filter((line: (String, String, String, String)) => !line._4.isEmpty)
      .map(row => BidItem(row._1, row._2, row._3, row._4.toDouble))
      .map(row => calculateEURPrice(row, exchangeRates))
      .map(row => setDateFormat(row))
  }

  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = sc.textFile(motelsPath)
    .map(row => (row.split(",")(0), row.split(",")(1)))

  def getMaxPrice(s1:String, v1:Double, s2:String, v2:Double): (String, Double) = {
    if(v1 >= v2)
      return (s1, v1)
    else
      return (s2, v2)
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val finalBids = bids.map(s => (s.motelId, s))
    finalBids.join(motels)
      .map(row => ((row._1, row._2._2, row.x._2.x._1.bidDate), (row.x._2.x._1.loSa, row.x._2.x._1.price))) //first divided into parts
      .reduceByKey((v1, v2) => getMaxPrice(v1._1, v1._2, v2._1, v2._2)) //find maximum
      .map(row => EnrichedItem(row._1._1, row._1._2, row._1._3, row._2._1, row._2._2)) // rejoin result
  }
}
