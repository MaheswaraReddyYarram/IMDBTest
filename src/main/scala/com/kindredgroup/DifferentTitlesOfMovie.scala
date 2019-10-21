package com.kindredgroup

import org.apache.spark.sql.functions.{col, broadcast}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DifferentTitlesOfMovie(sparkSession: SparkSession,
                             akasTitlesFilePath: String,
                             topMoviesDF: DataFrame,
                             inputFileFormat:String,
                             columnSeparator:String) {

  def getDifferentTitlesofMovie(): DataFrame = {

    //read akas titles file
    val akasTitlesDF = sparkSession
      .read
      .option("sep",columnSeparator)
      .option("header","true")
      .format(inputFileFormat)
      .load(akasTitlesFilePath)

    //join with top 10 movies dataframe to get different titles
    //broadcast top movies dataframe as it is small data set and to avoid shuffling of other dataframe
    val differentTitlesDF = akasTitlesDF.as("akastitlesdf")
      .join(broadcast(topMoviesDF.as("topmoviesdf")), col("topmoviesdf.tconst") === col("akastitlesdf.titleId"))
      .select("topMoviesdf.primaryTitle", "akastitlesdf.title")


    differentTitlesDF
  }

}
