package data
package db

/** 
 * Represents an implementation of a persistent store we can use to save the state
 * of our Scatter-Gather actors.
 */
trait StorageBackend {
  def open(): PersistentStore
}

/**
 * Represents a live, resource-acquired, instance of a Persistence store that
 * we can use to save our Scatter-Gather system.
 */
trait PersistentStore extends java.io.Closeable {
  /** Stores/Retrieves hotel information from disk. */
  def hotels: ValueStore[String, Hotel]
  /** Stores/Retrieves "Node -> Hotel ids" from disk. */
  def topics: ValueStore[String, Seq[String]]
  /** Stores/Retrieves "Node -> Sub Node ids" from disk. */
  def categories: ValueStore[String, Seq[String]]
  /** Closes all resources and shuts down this store. */
  def close(): Unit
}

/** A value store we can use to obtain/save data for use in our cluster. */
trait ValueStore[Key, Value] {
  def get(key: Key): Option[Value]
  def put(key: Key, value: Value): Unit
  // TODO - this one's not needed but for bootstrapping.
  def list: Seq[(Key, Value)]
}