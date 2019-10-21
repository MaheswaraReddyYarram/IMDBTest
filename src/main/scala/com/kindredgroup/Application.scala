package main.scala.com.kindredgroup

import com.kindredgroup.{DifferentTitlesOfMovie, MostCreditedPersons, TopNmovies}
import org.apache.spark.SparkConf
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


object Application {
  val LOGGER = LoggerFactory.getLogger(getClass)

  //Load configuration
  val appConfig: Config = ConfigFactory.load().resolve()

  def main(args: Array[String]): Unit = {

    //create spark session
    val sparkConf =new SparkConf().setAppName(appConfig.getString("spark.appName")).setMaster("local[2]")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    //read common properties
    val columnSeparator = appConfig.getString("spark.input.file.columns.separator")
    val inputFileFormat = appConfig.getString("spark.input.file.format")
    val outputFileFormat = appConfig.getString("spark.output.file.format")

    //read file paths
    val basicTitlesFilePath = appConfig.getString("spark.input.file.basics.titles.path")
    val ratingsFilePath = appConfig.getString("spark.input.file.ratings.path")
    val principalsFilePath = appConfig.getString("spark.input.file.principals.path")
    val basicNamesFilePath = appConfig.getString("spark.input.file.basic.names.path")
    val akasTitlesFilePath = appConfig.getString("spark.input.file.akas.titles.path")
    val outputPath = appConfig.getString("spark.output.path")

    //calculate top 10 movies
    val topNMovies: TopNmovies = new TopNmovies(sparkSession, basicTitlesFilePath, ratingsFilePath,
                                                inputFileFormat,columnSeparator)

    val topMoviesDF:DataFrame = topNMovies.calculateTop10Movies(10)
    //topMoviesDF.createOrReplaceTempView("temp")

    //val sqlContect = sparkSession.sql("select * from temp")
    //println(sqlContect)

    //printing top 10 movies as well as writing to file
    println("Top 10 Movies based on ranking is:")
    topMoviesDF.show()
    /*topMoviesDF.write.format(outputFileFormat)
      .option("header","true")
      .save(outputPath)*/

    //calculate most credited persons
    val mostCreditedPersons: MostCreditedPersons = new MostCreditedPersons(sparkSession, principalsFilePath,
                                    basicNamesFilePath, topMoviesDF, inputFileFormat, columnSeparator)

    val mostOftenCreditedDF:DataFrame = mostCreditedPersons.calculateMostCreditedPersons(10)

    println("Most Often Credited Persons are:")
    mostOftenCreditedDF.show()
    /*mostOftenCreditedDF.write.format(outputFileFormat)
      .option("header","true")
      .save(outputPath)*/

    //calculate different titles of top 10 movies
    val differentTitlesOfMovie = new DifferentTitlesOfMovie(sparkSession,akasTitlesFilePath,topMoviesDF, inputFileFormat,
                                     columnSeparator)

    val differentTitlesDF:DataFrame = differentTitlesOfMovie.getDifferentTitlesofMovie()

    println("Different titles of top 10 movies are:")
    differentTitlesDF.show(1000)

    /*differentTitlesDF.write.format(outputFileFormat)
      .option("header","true")
      .save(outputPath)*/


  }




}
