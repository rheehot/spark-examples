package com.leeyh0216.spark.example.datasource.library

import scala.collection.mutable

object Library {
  var _library: Library = new Library()

  def getLibrary(): Library ={
    _library
  }
}

class Library {
  val bookSheleves: mutable.Map[String, List[Book]] = mutable.Map()

  def addBookSeries(name: String, bookSeries: List[Book]): Unit =
    bookSheleves += (name -> bookSeries)

  def getBookSeries(name: String): List[Book] = {
    if(bookSheleves.contains(name))
      bookSheleves(name)
    else
      throw new IllegalArgumentException(s"No name book: ${name}")
  }
}
