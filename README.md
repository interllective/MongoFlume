MongoFlume allows Flume to write events using MongoDB as a sink. 

To use, include the a JAR file containing the MongoFlume code, as well as the Mongo Java Driver (found here) on the Flume CLASSPATH.

Then add the following to the Flume configuration in conf/flume-conf.xml:
```
  <property>
    <name>flume.plugin.classes</name>
    <value>com.interllective.mongoflume.MongoSink</value>
  </property>
```

To configure a Flume node to write to MongoDB, use the following sink:

```
mongo("host", "database", "collection")
```

This will write events to the MongoDB instance accessible via host, to the database.collection specified. 

By default, MongoFlume writes events using the SAFE MongoDB write concern, meaning it will wait for the server to acknowledge the write was successful. To change this default, specify a different write concern thus:

```
mongo("host", "database", "collection", "NORMAL")
```

Write concern can be one of: NONE NORMAL SAFE MAJORITY FSYNC_SAFE JOURNAL_SAFE REPLICAS_SAFE. For more info, see http://api.mongodb.org/java/current/
