package com.sylvaingoutouly.cb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class JointTest {
	
	private Bucket bucket;
	private CouchbaseCluster cluster;
	
	@Test
	public void shouldJoint() {
		try {
			final List<JsonDocument> docs = Joint.with(bucket)
				.from("abbaye_de_leffe-brune_brown")
				.to("subtype.links")
				.execute(true)
				.doOnError(e -> System.err.println(e.getMessage()))
				.doOnNext(doc -> System.out.println(doc.content().toString()))
				.toList()
				.toBlocking()
				.last();
			
			assertNotNull(docs);
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	
	
	@Before public void before() {
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();
		cluster = CouchbaseCluster.create(env, Arrays.asList("ec2-52-211-182-61.eu-west-1.compute.amazonaws.com"));
		bucket = cluster.openBucket("beer-sample");
	}
	
	@After public void after() {
		cluster.disconnect();
	}

}
