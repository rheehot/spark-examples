package com.leeyh0216.spark.example.datasource.inmemory

import com.leeyh0216.spark.example.datasource.library.Book
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

/**
 * Book 객체를 읽을 BookReader 객체를 생성한다.
 * @param book 읽어야 할 Book 객체
 */
class BookReaderFactory(book: Book) extends DataReaderFactory[Row]{
  override def createDataReader(): DataReader[Row] = new BookReader(book)
}
