package scattergather

import akka.actor.ActorRef
import data.Hotel

/**
 * A message representing a document to add to the search tree.
 */
case class AddHotel(content: Hotel)
/**
 * Represents a Search Query that is sent through the actors system.
 */
case class SearchQuery(query: String, maxDocs: Int)

/**
 * Represents a partial or full response of query results.
 */
case class QueryResponse(results: Seq[(Double, Hotel)], failed: Boolean = false) {
 override  def toString: String =
    if(failed) "Query Failed"
    else s"-- QueryResults --\n${results map (_._2) mkString "\n"}"
}