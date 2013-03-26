package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props,ActorLogging}


class AdaptiveSearchNode(val id: String = "top", val db: ActorRef) 
    extends Actor 
    with CategoryNode 
    with TopicNode 
    with Initializer 
    with debug.DebugActor
    with ActorLogging {
  // Start as a Topic Node.
  def receive = waitForInitialState
  
  // Make sure we load our behavior when starting
  // TODO - hold messages while waiting?
  override def preStart(): Unit = {
    log.debug("Loading initial state for node: " + id)
    loadInitialState()
  }

  def maxNoOfDocuments = 10
  def splitDocSize = 2
  def makeChild(childId: String): ActorRef =
    context.actorOf(
        Props(new AdaptiveSearchNode(childId, db)).withDispatcher("search-tree-dispatcher"),
        name = "search-node-" + childId)
  
  /** Splits this search node into a tree of search nodes if there are too many documents. */
  protected def split(): Unit = {
    println(s"Splitting node [$id]")
    // TODO - We should probably just keep acting like the Topic and wait for a new node to exist before telling our parent to replace us with the new node...
    import akka.pattern.{ask,pipe}
    import context.dispatcher
    import data.db.DbActor.{SaveTopic, SaveOk, SaveCategory}
    import concurrent.Future
    // Wait for new category initialization
    context become (waitForInitialState orElse debugHandler)
    // TODO - figure out better/more dynamic splits
    val childIdFutures: Seq[Future[String]] =
      for((docs, idx) <- (hotelIds grouped splitDocSize).zipWithIndex.toSeq) yield {
        val childId = id + "-" + idx
        (db ? SaveTopic(childId, docs)) map { ok => childId }
      }
    val futureChildIds = 
      for {
        children <- Future sequence childIdFutures
        // Make sure we save the category so we re-use it later...
        _ <- db ? SaveCategory(id, children)
      } yield children
    val initCategory = futureChildIds map InitCategory.apply
    pipe(initCategory) to context.self
    // Clear our current index
    clearIndex()
  }
}