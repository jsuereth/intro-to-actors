package data
package db

import com.sleepycat.je._
import com.sleepycat.bind.tuple.{
  TupleBinding,
  TupleOutput,
  TupleInput
}


final class BerkeleyBackend(dir: java.io.File) extends StorageBackend {
  def open(): PersistentStore = new BekeleyPersistentStore({
    val envConfig = new com.sleepycat.je.EnvironmentConfig
    envConfig setAllowCreate true
    //envConfig setCacheSize 1000000
    new Environment(dir, envConfig)
  })
}
object BerkeleyBackend {
  def default = new BerkeleyBackend({
      val tmp = new java.io.File("target/db")
      tmp.mkdirs()
      tmp
    })
}

/** This class implements the generic `PeristentStore` used for the examples wit BerkeleyDb. */
final class BekeleyPersistentStore(env: com.sleepycat.je.Environment) extends PersistentStore {
  val hotels = {
    val dbConfig = new DatabaseConfig()
    dbConfig setAllowCreate true
    val db = env.openDatabase(null, "hotelDb", dbConfig)
    new DatabaseHelper(db, HotelSerializer)
  }
  val topics = {
    val dbConfig = new DatabaseConfig()
    dbConfig setAllowCreate true
    val db = env.openDatabase(null, "topics", dbConfig)
    new DatabaseHelper(db, IdListSerializer)
  }
  val categories = {
    val dbConfig = new DatabaseConfig()
    dbConfig setAllowCreate true
    val db = env.openDatabase(null, "categories", dbConfig)
    new DatabaseHelper(db, IdListSerializer)
  }
  
  def close(): Unit = {
    // TODO - try catches...
    env.close()
    hotels.close()
    topics.close()
    categories.close()
  }
}

/** This class implements the generic backend interface `ValueStore` using a Berkeley db. */
class DatabaseHelper[Key, Value](db: Database, serializer: DbSerializer[Key,Value]) extends ValueStore[Key,Value] {
  def list(): Seq[(Key, Value)] = {
    val cursor = db.openCursor(null, null)
    val results = collection.mutable.ArrayBuffer.empty[(Key, Value)]
    try {
      val key = new DatabaseEntry
      val value = new DatabaseEntry
      def read(): Unit = cursor.getNext(key, value, LockMode.DEFAULT) match {
        case OperationStatus.SUCCESS =>
          results.append(serializer.deserialize(key, value))
          read()
        case _ => ()
      }
      read()
    } finally cursor.close()
    results.toSeq
  }
  def get(id: Key): Option[Value] = {
    val key = serializer serializeKey id
    val data = new DatabaseEntry
    db.get(null, key, data, LockMode.DEFAULT) match {
      case OperationStatus.SUCCESS => Some(serializer.deserialize(key, data)._2)
      case _ => None
    }
  }
  def put(key: Key, value: Value): Unit = {
    val (k, data) = serializer serialize (key, value)
    db.put(null, k, data)
  }
  def close(): Unit = db.close()
}

trait DbSerializer[Key, Value] {
  /** Serailize a value into a key-value pair. */
  def serialize(key: Key, value: Value): (DatabaseEntry, DatabaseEntry)
  def deserialize(key: DatabaseEntry, value: DatabaseEntry): (Key, Value)
  def serializeKey(key: Key): DatabaseEntry
}

abstract class StringKeySerializer[Value] extends DbSerializer[String, Value] {
  final val stringBinding = TupleBinding.getPrimitiveBinding(classOf[String])
  final def serializeKey(key: String): DatabaseEntry = {
    val entry = new DatabaseEntry
    stringBinding.objectToEntry(key, entry)
    entry
  }
}
/** A serializer interface for hotels. */
object HotelSerializer extends StringKeySerializer[Hotel] {
  def serialize(key: String, hotel: Hotel): (DatabaseEntry, DatabaseEntry) = {
    val entry = new DatabaseEntry
    HotelBinding.objectToEntry(hotel, entry)
    serializeKey(key) -> entry
  }
  def deserialize(key: DatabaseEntry, value: DatabaseEntry): (String, Hotel) =
    stringBinding.entryToObject(key) -> HotelBinding.entryToObject(value)
  /** Helper object for going to/from DatabaseEntry. */  
  private object HotelBinding extends TupleBinding[Hotel] {
    def objectToEntry(hotel: Hotel, to: TupleOutput): Unit = {
        to writeString hotel.id
        to writeString hotel.name
        to writeString hotel.description
        to writeString hotel.location.address
        to writeString hotel.location.city
        to writeString hotel.location.country
    }
    def entryToObject(ti: TupleInput): Hotel = {
        val id = ti.readString
        val name = ti.readString
        val description = ti.readString
        val location = 
          Location(ti.readString, ti.readString, ti.readString)
        Hotel(id,name,description, location)
    }
  }
}

object IdListSerializer extends StringKeySerializer[Seq[String]] {
  def serialize(key: String, ids: Seq[String]): (DatabaseEntry, DatabaseEntry) = {
    val entry = new DatabaseEntry
    IdListBinding.objectToEntry(ids, entry)
    serializeKey(key) -> entry
  }
  def deserialize(key: DatabaseEntry, value: DatabaseEntry): (String, Seq[String]) =
    stringBinding.entryToObject(key) -> IdListBinding.entryToObject(value)
  private object IdListBinding extends TupleBinding[Seq[String]] {
    def objectToEntry(list: Seq[String], to: TupleOutput): Unit = {
      to writeInt list.length
      var idx = 0
      while(idx < list.length) {
        to writeString list(idx)
        idx += 1
      }
    }
    def entryToObject(ti: TupleInput): Seq[String]= {
      val length = ti.readInt
      for(i <- 0 until length) 
      yield ti.readString
    }
  }
}