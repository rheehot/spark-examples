package com.leeyh0216.spark.example.datasource.inmemory

import java.util.UUID

import com.leeyh0216.spark.example.datasource.library.{Book, Library, Page}
import org.apache.spark.sql
import org.junit.{Assert, Test}
import org.slf4j.LoggerFactory

import scala.util.Random

class InmemoryDataSourceTest {

  val logger = LoggerFactory.getLogger(classOf[InmemoryDataSourceTest])
  val library = Library.getLibrary()

  val harryPotterVolumes = 7
  val harryPotterBookName = "Harry Potter"
  val harryPotter = (1 to harryPotterVolumes).map(volume => {
    val lastPage = new Random().nextInt() % 300
    val pages = (1 to lastPage).map(page => Page(page, UUID.randomUUID().toString))
    Book(harryPotterBookName, volume, pages.toList)
  })
  val sumOfHarryPotterPages = harryPotter.map(book => book.pages).map(pages => pages.length).sum
  library.addBookSeries(harryPotterBookName, harryPotter.toList)

  val spark = new sql.SparkSession.Builder().master("local[*]").config("spark.driver.host", "localhost").getOrCreate()
  val inmemoryDataSourceName = classOf[BookDataSource].getName

  @Test
  def testGetNumPartitions(): Unit ={
    val harryPotters = spark.read.format(inmemoryDataSourceName).option("bookName", "Harry Potter").load()
    Assert.assertEquals(harryPotterVolumes, harryPotters.rdd.getNumPartitions)
  }

  @Test
  def testCount(): Unit ={
    val harryPotters = spark.read.format(inmemoryDataSourceName).option("bookName", "Harry Potter").load()
    Assert.assertEquals(sumOfHarryPotterPages, harryPotters.count())
  }
}
