Scalandra
=========

Scalandra is a Scala wrapper for Cassandra's Thrift API. We currently target Cassandra 0.5.

Data in Cassandra is essentially a huge multi-dimensional map. Scalandra aims to provide a map-like interface to Cassandra with all the bells and whistles supported by Cassandra API.

Scaladoc is located at http://nodeta.github.com/scalandra/.

Features
--------

* works with Cassandra 0.5
* treat Cassandra as a huge Scala Map
* connection pool for efficient connectivity
* (de)serialization API for easy manipulation

Example
-------

### Cassandra access and manipulation

<pre><code class="scala">
    import com.nodeta.scalandra._
    import com.nodeta.scalandra.serializer.StringSerializer
    
    val serialization = new Serialization(
      StringSerializer,
      StringSerializer,
      StringSerializer
    )
    val cassandra = new Client(
      Connection("127.0.0.1", 9162),
      "Keyspace1",
      serialization,
      ConsistencyLevels.one
    )
    
    cassandra.ColumnFamily("Standard1")("row")("column1") = "value"
    cassandra.ColumnFamily("Standard1")("row")("column2") = "value"
    cassandra.ColumnFamily("Standard1")("row")("column3") = "value"
    // or just cassandra.ColumnFamily("Standard1")("row") = Map("column1" -> "value", ...)
    
    cassandra.ColumnFamily("Standard1")("row")("column1")
    // => "value"

    val range = Range(Some("column2"), None, Ascending, 100)
    cassandra.ColumnFamily("Standard1")("row").slice(range)
    // => Map("column2" -> "value", "column3" -> "value")
    
    cassandra.ColumnFamily("Standard1")("row").slice(List("column1", "column2"))
    // => Map("column1" -> "value", "column3" -> "value")
</code></pre>

### Connection Pool

For more complicate applications, connection pooling is usually necessary. Scalandra provides simple and type-safe connection pool based on Apache Commons Pool.

<pre><code class="scala">
    import com.nodeta.scalandra.ConnectionProvider
    val pool = new ConnectionProvider("127.0.0.1", 9160)
    
    pool { connection =>
        val client = new Client(
          connection,
          "Keyspace",
          serialization,
          ConsistencyLevels.default
        )
        // do something
    }
</code></pre>

Running tests
-------------

Cassandra tests can be run using <code>rake test</code>, which setups and runs a suitable Cassandra instance for testing purposes.

Future development
------------------

* API for batch mutations
* Scala 2.8 support
* Add support for multiple hosts in connection pool

