package ch.epfl.lts2

/**
  * Created by volodymyrmiz on 30/07/18.
  */
package object Globals {
  // timestamps are in days, e.g. 2221 days in total
  val START_TIME = 0 // start date: 06 Jan 1998
  val END_TIME = 2221 // end date: 04 Feb 2004

  val DEC_99_START = 359 + 365 - 31 // 359 days in 1998 + 365 days in 1999 - 31 days of December
  val DEC_99_END = 359 + 365 // 359 days in 1998 + 365 days in 1999
}
