package ch.epfl.lts2

/**
  * Created by volodymyrmiz on 30/07/18.
  */
package object Globals {

//  val PATH_RESOURCES: String = "/mnt/data/git/enron-email-network-analysis/EnronNet/src/main/resources/"
  val PATH_RESOURCES: String = "/home/volodymyrmiz/git/enron-email-network-analysis/EnronNet/src/main/resources/"

  // timestamps are in days, e.g. 2221 days in total
  val START_TIME: Int = 0 // start date: 06 Jan 1998
  val DAYS_TOTAL: Int = 2221 // end date: 04 Feb 2004

  /*
  The following dates are for evaluation purposes.

  The paper we are comparing to is "Locality statistics for anomaly detection in time
  series of graphs" https://arxiv.org/pdf/1306.0267.pdf
  The events are described in the Section VII of the paper.
  */
  // December
  val DEC_99_START: Int = 359 + 365 - 31 // 359 days in 1998 + 365 days in 1999 - 31 days of December
  val DEC_99_END: Int = 359 + 365 // 359 days in 1998 + 365 days in 1999

  // Mid-August 2001
  val AUG_01_START: Int = 359 + 365 + 366 + 213 // 359 in 1998 + 365 in 1999 + 366 in 2000 + 213 in 2001
  val AUG_01_END: Int = 359 + 365 + 366 + 244

  // Late April 2001
  val APR_01_START: Int = 359 + 365 + 366 + 91
  val APR_01_END: Int = 359 + 365 + 366 + 121

  // End of May 2001 (It is said "before June 2001" in the paper, so add a few days of June as well (here, until 7 June))
  val MAY_01_START: Int = 359 + 365 + 366 + 122
  val MAY_01_END: Int = 359 + 365 + 366 + 159
}
