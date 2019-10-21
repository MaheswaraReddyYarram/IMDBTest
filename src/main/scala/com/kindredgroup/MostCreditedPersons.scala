package com.kindredgroup

import org.apache.spark.sql.functions.{col, broadcast}
import org.apache.spark.sql.{DataFrame, SparkSession}

class MostCreditedPersons(sparkSession: SparkSession,
                          principalsFilePath: String,
                          basicNamesFilePath:String,
                          topMoviesDF: DataFrame,
                          inputFileFormat:String,
                          columnSeparator:String) {

  def calculateMostCreditedPersons(top: Int): DataFrame = {

    //read principals titles file
    val principalsDF = sparkSession
      .read
      .option("sep",columnSeparator)
      .option("header","true")
      .format(inputFileFormat)
      .load(principalsFilePath)


    //broadcast top movies dataframe as it is small data set and to avoid shuffling of other dataframe
    val principalCreditedDF = principalsDF.as("prinicipalsdf")
      .join(broadcast(topMoviesDF.as("topMoviesdf")),
        col("prinicipalsdf.tconst") === col("topMoviesdf.tconst"))
      .select("prinicipalsdf.nconst", "topMoviesdf.primaryTitle")


    //read basic names file
    val basicsDF = sparkSession
      .read
      .option("sep",columnSeparator)
      .option("header","true")
      .format(inputFileFormat)
      .load(basicNamesFilePath)

    //get most often credited persons

    val mostOftenCreditedDF = principalCreditedDF.as("principalcrediteddf")
      .join(basicsDF.as("basicsdf"), col("principalcrediteddf.nconst") === col("basicsdf.nconst"))
      .select("principalCreditedDF.primaryTitle", "basicsDF.primaryName").limit(top)

    mostOftenCreditedDF
  }

}
