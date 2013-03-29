package debug

import akka.actor.Actor
import akka.actor.actorRef2Scala
import akka.actor.ActorRef

case object GetName

trait DebugActor extends Actor {
  def debugHandler: Receive = {
    case GetName => 
      // TODO - Aggregate this krap and make a nice tree.
      sender ! context.self
      debugChildren foreach (_.tell(GetName, sender))
  }
  
  def debugChildren: Iterable[ActorRef] = context.children
}