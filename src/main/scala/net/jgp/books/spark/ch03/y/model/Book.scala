package net.jgp.books.spark.ch03.y.model

import java.util.Date

case class Book(authorId:Int, title:String, releaseDate:Date, link:String, id:Int=0)
