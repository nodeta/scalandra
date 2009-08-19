package com.nodeta.scalandra.client

trait ReadWrite[A, B, C] extends Read[A, B, C] with Write[A, B, C] { this : Base[A, B, C] =>
  /**
   * Remove all entries from given column family
   * 
   * @throws UnsupportedOperationException Works only when an order preserving partitioner is used.
   */
  def truncate(columnFamily : String) {
    keys(columnFamily, None, None).foreach(key => this.remove(ColumnParent[A](columnFamily, key)))
  }
}