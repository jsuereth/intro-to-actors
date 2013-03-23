package data
package db

import com.sleepycat.je.{
  DatabaseEntry,
  Database
}
import com.sleepycat.bind.tuple.{
  TupleBinding,
  TupleOutput,
  TupleInput
}

// TODO - Clean this up *A LOT*
object Db {
  import com.sleepycat.je._
  private var env: Option[Environment] = None
  private var hotelDb: Option[DatabaseHelper[String, Hotel]] = None
  private var topicListDb: Option[DatabaseHelper[String, IdList]] = None
  private var categoryListDb: Option[DatabaseHelper[String, IdList]] = None
  def open(): Unit = {
    val dbDir = new java.io.File("target/db")
    dbDir.mkdirs()
    val envConfig = new EnvironmentConfig()
    envConfig setAllowCreate true
    env = Some(new Environment(dbDir, envConfig));

    // Open the database. Create it if it does not already exist.
    val dbConfig = new DatabaseConfig()
    dbConfig setAllowCreate true
    hotelDb = env map (_.openDatabase(null, "hotelDb", dbConfig)) map (db => new DatabaseHelper(db, HotelSerializer))
    topicListDb = env map (_.openDatabase(null, "topicDb", dbConfig)) map (db => new DatabaseHelper(db, IdListSerializer))
    categoryListDb = env map (_.openDatabase(null, "categoryDb", dbConfig)) map (db => new DatabaseHelper(db, IdListSerializer))
  }
  def topic = topicListDb.get
  def category = categoryListDb.get
  def hotel = hotelDb.get
  def close(): Unit = {
    env foreach (_.close)
    env = None
    hotelDb foreach (_.close)
    hotelDb = None
  }
}

class DatabaseHelper[Key, Value](db: Database, serializer: DbSerializer[Key,Value]) {
  import com.sleepycat.je._
  def read(): Seq[Value] = {
    val cursor = db.openCursor(null, null)
    val results = collection.mutable.ArrayBuffer.empty[Value]
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
      case OperationStatus.SUCCESS => Some(serializer.deserialize(key, data))
      case _ => None
    }
  }
  def put(value: Value): Unit = {
    val (key, data) = serializer serialize value
    db.put(null, key, data)
  }
  def close(): Unit = db.close()
}

trait DbSerializer[Key, Value] {
  /** Serailize a value into a key-value pair. */
  def serialize(value: Value): (DatabaseEntry, DatabaseEntry)
  def deserialize(key: DatabaseEntry, value: DatabaseEntry): Value
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
  def serialize(hotel: Hotel): (DatabaseEntry, DatabaseEntry) = {
    val entry = new DatabaseEntry
    HotelBinding.objectToEntry(hotel, entry)
    serializeKey(hotel.id) -> entry
  }
  def deserialize(key: DatabaseEntry, value: DatabaseEntry): Hotel =
    HotelBinding.entryToObject(value)
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

object IdListSerializer extends StringKeySerializer[IdList] {
  def serialize(ids: IdList): (DatabaseEntry, DatabaseEntry) = {
    val entry = new DatabaseEntry
    IdListBinding.objectToEntry(ids, entry)
    serializeKey(ids.id) -> entry
  }
  def deserialize(key: DatabaseEntry, value: DatabaseEntry): IdList =
    IdListBinding.entryToObject(value)
  private object IdListBinding extends TupleBinding[IdList] {
    def objectToEntry(list: IdList, to: TupleOutput): Unit = {
      to writeString list.id
      to writeInt list.ids.length
      var idx = 0
      while(idx < list.ids.length) {
        to writeString list.ids(idx)
      }
    }
    def entryToObject(ti: TupleInput): IdList = {
      val id = ti.readString
      val length = ti.readInt
      val ids =
        for(i <- 0 until length) 
        yield ti.readString
      IdList(id, ids)
    }
  }
}