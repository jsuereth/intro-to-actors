package data
package db

import com.basho.riak.client.{
  IRiakClient,
  RiakFactory
}
import com.basho.riak.client.bucket.Bucket

object RiakBackend {
  def default: StorageBackend = new RiakBackened
}
class RiakBackened extends StorageBackend {
  def open(): PersistentStore = new RiakStore(RiakFactory.pbcClient)
}



trait Stringifier[K] {
  def fromString(string: String): K
  def toString(value: K): String
}
object Stringifier {
  implicit object stringStr extends Stringifier[String] {
      def fromString(string: String): String = string
      def toString(value: String): String = value
  }
  // TODO - abstract this sucker for all case classes...
  implicit object hotelStr extends Stringifier[Hotel] {
    private val SPLITTER="^"
    private val SPLITTER_REGEX="\\^"
    def toString(value: Hotel): String = {
      val buffer = new StringBuffer
      buffer.append(value.id).append(SPLITTER)
      buffer.append(value.name).append(SPLITTER)
      buffer.append(value.description).append(SPLITTER)
      buffer.append(value.location.address).append(SPLITTER)
      buffer.append(value.location.city).append(SPLITTER)
      buffer.append(value.location.country)
      buffer.toString()
    }
    def fromString(string: String): Hotel = {
      string split SPLITTER_REGEX match {
        case Array(id, name, description, address, city, country) =>
          new Hotel(id, name, description, Location(address,city,country))
        case _ => sys.error(s"Could not deserialize hotel from string: $string")
      }
    }
  }
    // TODO - This isn't a very good serialization mechanism...
  implicit object seqStr extends Stringifier[Seq[String]] {
    def fromString(string: String): Seq[String] = 
      string split ","
    def toString(value: Seq[String]): String = 
      value mkString ","
  }
}

class RiakStore(client: IRiakClient) extends PersistentStore {
  
  private def getOrFindBucket(name: String): Bucket = {
    if(client.listBuckets contains name) client.fetchBucket(name).execute()
    else client.createBucket(name).execute()
  }
  
  def hotels: ValueStore[String, Hotel] = new RiakBucket(getOrFindBucket("hotels"))
  def topics: ValueStore[String, Seq[String]] = new RiakBucket(getOrFindBucket("topics"))
  def categories: ValueStore[String, Seq[String]] = new RiakBucket(getOrFindBucket("categories"))
  def close(): Unit  = client.shutdown()
}

class RiakBucket[Key, Value](bucket: Bucket)(implicit k: Stringifier[Key], v: Stringifier[Value]) 
    extends ValueStore[Key, Value] {
  def get(key: Key): Option[Value] = {
    val keyString = k toString key
    val result = bucket.fetch(keyString).notFoundOK(true).execute()
    Option(result) map (_.getValueAsString()) map v.fromString
  }
  def put(key: Key, value: Value): Unit = {
    val keyString = k toString key
    val valueString = v toString value
    bucket.store(keyString, valueString).execute()
  }
  
  import collection.JavaConverters._
  def list: Seq[(Key, Value)] = 
    for {
      keyString <- bucket.keys.asScala.toSeq
      key = k fromString keyString
      value <- get(key).toSeq
    } yield key -> value
    
}

