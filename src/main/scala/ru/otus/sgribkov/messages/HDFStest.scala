package ru.otus.sgribkov.messages

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI


object HDFStest extends App {

  val conf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf)
  val folderPath = new Path("/data/messages/details") //1616527800
  //fs.mkdirs(new Path("/data/messages/details"))
  //fs.delete(folderPath)

  val files = fs.listStatus(folderPath)

  files.foreach( x => println(x.getPath ))

}
