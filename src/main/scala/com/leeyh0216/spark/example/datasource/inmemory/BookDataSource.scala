package com.leeyh0216.spark.example.datasource.inmemory

import com.leeyh0216.spark.example.datasource.library.Library
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.slf4j.LoggerFactory

/**
 * 커스텀 데이터소스인 BookDataSource 클래스.
 * 읽기 전용 DataSource이기 때문에 ReadSupport만 상속하였다.
 */
class BookDataSource extends DataSourceV2 with ReadSupport{
  val logger = LoggerFactory.getLogger(classOf[BookDataSource])

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val bookName = options.get("bookName").get()
    new BookDataSourceReader(Library.getLibrary(), bookName)
  }
}
