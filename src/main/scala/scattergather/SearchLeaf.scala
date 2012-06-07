package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}

trait SearchLeaf { self: AdaptiveSearchNode =>
  final val maxNoOfDocuments = 10
  var documents: Vector[String] = Vector()
  var index: HashMap[String, Seq[(Double, String)]] = HashMap()

  
  def leafNode: PartialFunction[Any, Unit] = {
    // hacks to excercise behavior
    case SearchQuery("BAD", _, r) => r ! QueryResponse(Seq.empty, failed=true)
    case SearchQuery(query, maxDocs, handler) => executeLocalQuery(query, maxDocs, handler)
    case SearchableDocument(content)          => addDocumentToLocalIndex(content)
  }
  
  
  private def executeLocalQuery(query: String, maxDocs: Int, handler: ActorRef) = {
    val result = for {
      results <- (index get query).toSeq
      resultList <- results
    } yield resultList
    handler ! QueryResponse(result take maxDocs)
  }

  private def addDocumentToLocalIndex(content: String) = {
    documents = documents :+ content
    // Split on size or add to index.
    if (documents.size > maxNoOfDocuments) split()
    else {
      for( (key,value) <- content.split("\\s+").groupBy(identity)) {
        val list = index.get(key) getOrElse Seq()
        index += ((key, ((value.length.toDouble, content)) +: list))
      }
    }
  }

  /** Abstract method to split this actor. */
  protected def split(): Unit

  protected def clearIndex(): Unit = {
    documents = Vector()
    index = HashMap()
  }
}