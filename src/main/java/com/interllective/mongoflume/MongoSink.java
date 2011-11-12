package com.interllective.mongoflume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;


public class MongoSink extends EventSink.Base {

	private DBCollection coll;
	private WriteConcern concern = WriteConcern.SAFE;
	private Mongo mongo;

	private String host;
	private String databaseName;
	private String collName;
	
	
	public MongoSink(String host, String database, String coll, String concern) {
		
		Preconditions.checkNotNull(host, "Must specify Mongo host name.");
	    Preconditions.checkNotNull(database, "Must specify Mongo database. ");
	    Preconditions.checkNotNull(coll, "Must specify Mongo collection. ");
	    
	    this.host = host;
	    databaseName = database;
	    collName = coll;
	    if(concern != null) {
	    	this.concern = WriteConcern.valueOf(concern);
	    }
	}
	
	public void append(Event event) throws IOException, InterruptedException {

		BasicDBObject obj = new BasicDBObject();

		// add metadata
		for(Entry<String, byte[]> kv : event.getAttrs().entrySet()) {
			obj.put(kv.getKey(), new String(kv.getValue()));
		}

		obj.put("body", new String(event.getBody()));

		obj.put("ts",event.getTimestamp());

		// TODO: implement group commit - with threads?
		coll.insert(obj, concern);

	}

	synchronized public void close() throws IOException, InterruptedException {
		mongo.close();
	}

	synchronized public void open() throws IOException, InterruptedException {

		mongo = new Mongo(host);

		DB db = mongo.getDB(databaseName);
		coll = db.getCollection(collName);

	}


	public static SinkBuilder builder() {
		return new SinkBuilder() {
			// construct a new parameterized sink
			@Override
			public EventSink build(Context context, String... argv) {
				Preconditions.checkArgument(argv.length > 2,
						"usage: mongoSink(\"host\", \"database\", \"collection\", [writeConcern])");

				String host = argv[0];
				String db = argv[1];
				String coll = argv[2];
				String wc = "SAFE";
				
				if(argv.length > 3) {
					wc = argv[3];
				}
				
				return new MongoSink(host, db, coll, wc);
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this class
	 * as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
		List<Pair<String, SinkBuilder>> builders =
				new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("mongo", builder()));
		return builders;
	}

}

