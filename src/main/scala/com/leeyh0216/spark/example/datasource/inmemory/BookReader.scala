package com.leeyh0216.spark.example.datasource.inmemory

import com.leeyh0216.spark.example.datasource.library.Book
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.slf4j.LoggerFactory

/**
 * 실제로 Book을 읽어들이는 BookReader 클래스.
 * Executor에서 동작하게 된다.
 * @param book 읽어야 할 Book 객체
 */
class BookReader(book: Book) extends DataReader[Row]{
  val logger = LoggerFactory.getLogger(classOf[BookReader])
  val pageIterator = book.pages.iterator
  val name = book.name
  val volume = book.volume

  logger.info(s"Book reader initialized. Name: ${book.name}, Volume: ${book.volume}")

  override def next(): Boolean = {
    pageIterator.hasNext
  }

  override def get(): Row = {
    val page = pageIterator.next()
    Row.apply(name, volume, page.page, page.content)
  }

  override def close(): Unit = {
    logger.info(s"Book closed. Name: ${book.name}, Volume: ${book.volume}")
  }
}
