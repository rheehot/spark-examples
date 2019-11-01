package com.leeyh0216.spark.example.datasource.inmemory

import java.util

import com.leeyh0216.spark.example.datasource.library.Library
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.slf4j.LoggerFactory

/**
 * DataSourceReader를 상속한 BookDataSourceReader 클래스.
 * Library에 있는 BookShelves에서 특정 Book을 읽는 것을 지원한다.
 *
 * @param library Library 객체
 * @param name 읽을 책 이름
 */
class BookDataSourceReader(library: Library, name: String) extends DataSourceReader {
  val logger = LoggerFactory.getLogger(classOf[BookDataSourceReader])
  val booksToRead = library.getBookSeries(name)

  override def readSchema(): StructType =
    new StructType()
      .add("name", StringType)
      .add("volume", IntegerType)
      .add("page", IntegerType)
      .add("content", StringType)

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val bookReaderFactories = new util.ArrayList[DataReaderFactory[Row]]()
    booksToRead.foreach(book => bookReaderFactories.add(new BookReaderFactory(book)))
    bookReaderFactories
  }
}
