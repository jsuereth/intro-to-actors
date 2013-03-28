package scattergather

import akka.actor._
import akka.util.Timeout
import java.util.concurrent.TimeUnit

class StatisticsCollector extends Actor with ActorLogging {
  import StatisticsCollector._
  var avgQueryTime: Double = 0.0
  var firstQueryTime = true
  
  def receive: Receive = {
    case stat: QueryStat => updateStats(stat)
  }
  
  def updateStats(stat: QueryStat) = {
    stat match {
     // These are heuristics to make our calculuation better...
    case FastQuery =>
      if(avgQueryTime > 1.0) {
        avgQueryTime -= 1.0
      }
    case TimeOut =>
      avgQueryTime += 3000.0;
    case QueryTime(ms) =>
      if(firstQueryTime) {
        // TODO - only do on startup...
        avgQueryTime = ms
      } else {
        avgQueryTime = (avgQueryTime + ms) / 2.0
      }
    }
    firstQueryTime = false
    log.debug(f"Average Query time = $avgQueryTime%02.2f")
  }
}
object StatisticsCollector {
  sealed trait QueryStat
  case object FastQuery extends QueryStat
  case class QueryTime(ms: Long) extends QueryStat
  case object TimeOut extends QueryStat
}


class StatisticsInterceptor(stats: ActorRef, from: ActorRef, to: ActorRef, msg: Any) extends Actor {
  val startTime = java.lang.System.currentTimeMillis
  to ! msg
  val timeout = Timeout(3, TimeUnit.SECONDS)
  def receive: Receive = {
    case response: QueryResponse => 
      from ! response
      updateStats()
      context stop self
    case ReceiveTimeout =>
      // TODO - send bad response?
      stats ! StatisticsCollector.TimeOut
      context stop self
  }
  context setReceiveTimeout timeout.duration
  
  def updateStats(): Unit = {
    val stopTime = java.lang.System.currentTimeMillis
    val total = stopTime - startTime
    val msg = 
      if(total == 0) StatisticsCollector.FastQuery
      else StatisticsCollector.QueryTime(total)
    stats ! msg
  }
}




