package scattergather

import akka.actor.ActorRef


/**
 * A message representing a document to add to the search tree.
 */
case class SearchableDocument(content: String)
/**
 * Represents a Search Query that is sent through the actors system.
 */
case class SearchQuery(query: String, maxDocs: Int, gatherer: ActorRef)

/**
 * Represents a partial or full response of query results.
 */
case class QueryResponse(results: Seq[(Double, String)], failed: Boolean = false)