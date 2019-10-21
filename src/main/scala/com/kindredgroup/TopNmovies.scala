package com.kindredgroup

import org.apache.spark.sql.functions.{avg, col, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TopNmovies(sparkSession: SparkSession,
                 basicTitleFilePath: String,
                 ratingsFilePath: String,
                 inputFileFormat:String,
                 columnSeparator:String) {

  def calculateTop10Movies(top: Int): DataFrame = {
    //read basic titles file and only movie records
    val basicTitlesDF= sparkSession
      .read
      .option("sep",columnSeparator)
      .option("header","true")
      .format(inputFileFormat)
      .load(basicTitleFilePath)
      .filter(col("titleType") === "movie")

    //read ratings file and records whose numVotes value is >=50
    var ratingsDF : DataFrame = sparkSession
      .read
      .option("sep",columnSeparator)
      .option("header","true")
      .format(inputFileFormat)
      .load(ratingsFilePath)
      .filter(col("numVotes") >= 50)

    //calculate average no of votes
    var avgNoOfVotes = ratingsDF.select(avg(col("numVotes")).as("averageVotes")).collect()
    var avgVotes: Double = 0
    avgNoOfVotes.foreach( t => { avgVotes = t.get(0).asInstanceOf[Double] } )

    sparkSession.sparkContext.broadcast(avgVotes)

    //add a new column rank to ratings dataframe
    ratingsDF = ratingsDF.withColumn("rank",
      (ratingsDF("numVotes")/ avgVotes.doubleValue()) * ratingsDF("averageRating"))

    // sort ratings dataframe based on rank column
    ratingsDF = ratingsDF.sort(desc("rank"))

    // join ratingsa nd titles data frame, order by rank, select top 10 rows,
    val topMoviesDF = ratingsDF.as("ratingsdf").join(basicTitlesDF.as("titlesdf"),
      col("titlesdf.tconst") === col("ratingsdf.tconst")).orderBy(desc("ratingsDF.rank"))
      .select("titlesdf.primaryTitle", "titlesdf.tconst").limit(top)
    //println("Top 10 movies are:")
    //topMoviesDF.show()
    topMoviesDF
  }

}
