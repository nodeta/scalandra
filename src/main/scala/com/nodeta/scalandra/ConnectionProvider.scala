package com.nodeta.scalandra

/**
 * Factory for Pooled Connection objects.
 *
 * @author Ville Lautanala
 */
case class ConnectionProvider(host : String, port : Int) extends pool.Factory[Connection] {
  def build() = {
    new Connection(host, port)
  }

  def destroy(c : Connection) = c.close
  def validate(c : Connection) = c.isOpen
  def activate(c : Connection) = { if (!c.isOpen) c.open }
  def passivate(c : Connection) = c.flush
}
