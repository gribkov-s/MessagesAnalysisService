package ru.otus.sgribkov.messages

import net.liftweb.json.{DefaultFormats, parse}

trait ParseJson {

  def getCaseClass[T: Manifest](jsonData: String): T  = {

    implicit val formats = DefaultFormats

    val json = parse(jsonData)
    json.extract[T]
  }
}
