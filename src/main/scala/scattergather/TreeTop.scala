package scattergather

import akka.actor._
import data.Hotel
import debug.DebugActor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp

/**
 * This actor acts inside the cluster to ensure that we notify every front-end actor where
 * the root node for the tree is.
 */
class TreeTop(val id: String, val db: ActorRef) extends Actor with ActorLogging with DebugActor {
  import NodeManager._
  val tree = context.actorOf(Props(new NodeManager(id, db)), "node-manager-"+id)
  val cluster = Cluster(context.system)
  
  
  def receive: Receive = {
    case q: SearchQuery => tree.tell(q, sender)
    case h: AddHotel => tree.tell(h, sender)
    case MemberUp(member) =>
      // A new member in the cluster!
      log.info(s"Found new cluster to register backend on: $member")
      // Register the root node (us)
      context.actorFor(RootActorPath(member.address) / "user" / "search-front-end") ! frontend.RegisterTree(self)
  }
  
  // TODO - ping roland about this one.
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  
  override def postStop(): Unit = cluster.unsubscribe(self)
}

