package ru.otus.sgribkov.messages

case class InputMessage(id: Long,
                        language_code: String,
                        content: String,
                        originator: Originator,
                        tags: List[String],
                        time: Option[Long]
                       )

case class Originator(id: Long,
                      name: String,
                      url: String
                     )

