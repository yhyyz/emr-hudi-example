package com.aws.analytics.cdc.const

/**
 * A required field that contains a string value describing the type of operation.
 * Values for the MySQL connector are c for create (or insert), u for update,
 * d for delete, and r for read (in the case of a snapshot).
 */
object DebeziumOP {
  val INSERT = "c"
  val UPDATE = "u"
  val DELETE = "d"
  val SNAPSHOT = "r"
}
