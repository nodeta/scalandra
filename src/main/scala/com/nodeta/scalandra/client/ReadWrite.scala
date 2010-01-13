package com.nodeta.scalandra.client

trait ReadWrite[A, B, C] extends Read[A, B, C] with Write[A, B, C] { this : Base[A, B, C] =>
  /**
   * Remove all entries from given column family
   * 
   * @throws UnsupportedOperationException Works only when an order preserving partitioner is used.
   */
  def truncate(_columnFamily : String) {
    val _serializer = serializer
    keys(_columnFamily, None, None).foreach(key => this.remove(key, new Path[A, B] {
      val serializer = _serializer
      val columnFamily = _columnFamily
    }))
  }
}
