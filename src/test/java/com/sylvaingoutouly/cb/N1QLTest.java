package com.sylvaingoutouly.cb;

import static com.couchbase.client.java.query.N1qlQuery.simple;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.dsl.path.index.IndexType;

public class N1QLTest {

	private Bucket bucket;
	private CouchbaseCluster cluster;

	@Test
	public void shouldCreatePrimaryIndex() {
		try {
			N1QL.with(bucket).dropPrimaryIndex();
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.createPrimaryIndex();
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void shouldDropPrimaryIndex() {
		try {
			N1QL.with(bucket).createPrimaryIndex();
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.dropPrimaryIndex();
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void shouldCreateNamedPrimaryIndex() {
		try {
			N1QL.with(bucket).dropPrimaryIndex("monIndex");
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.createPrimaryIndex("monIndex");
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void shouldDropNamedPrimaryIndex() {
		try {
			N1QL.with(bucket).createPrimaryIndex("monIndex");
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.dropPrimaryIndex("monIndex");
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void shouldCreateSecondaryIndex() {
		try {
			N1QL.with(bucket).dropSecondaryIndex("monIndexSecondaire");
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.createSecondaryIndex("monIndexSecondaire", "fr.laposte.disf.Toto", "_class");
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void shouldCreateCoveringSecondaryIndex() {
		try {
			N1QL.with(bucket).dropSecondaryIndex("monIndexSecondaireCouvrant");
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.createSecondaryIndex("monIndexSecondaireCouvrant", "fr.laposte.disf.Toto", "_class", "champSupp1", "champSupp2");
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void shouldDropSecondaryIndex() {
		try {
			N1QL.with(bucket)
				.createSecondaryIndex("monIndexSecondaire", "fr.laposte.disf.Toto", "_class");
		}
		catch (Exception e) {}
		
		try {
			N1QL.with(bucket)
				.dropSecondaryIndex("monIndexSecondaire");
		}
		catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void shouldQuery() {
		try {
			final List<JsonObject> datas = N1QL.with(bucket)
				.query("select * from %bucket%")
				.execute();
			
			assertNotNull(datas);
			assertTrue(datas.size() > 0);
			
			System.out.println(datas);
		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

	@Test
	public void shouldQueryAsync() {
		try {
			final List<JsonObject> datas = N1QL.with(bucket)
				.async()
				.query("select * from %bucket%")
				.execute()
				.toList()
				.toBlocking()
				.singleOrDefault(null);

			assertNotNull(datas);
			assertTrue(datas.size() > 0);

			System.out.println(datas);
		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

	@Test
	public void shouldQueryEntityAsync() {
		try {
			final List<JsonObject> datas = N1QL.with(bucket)
				.async()
				.queryEntity(Entity.class, "field1", "field2")
				.execute()
				.toList()
				.toBlocking()
				.singleOrDefault(null);

			assertNotNull(datas);
			assertTrue(datas.size() > 0);

			System.out.println(datas);
		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	@Test
	public void shouldQueryEntityAsyncAndMap() {
		try {
			final List<Entity> datas = N1QL.with(bucket)
				.async()
				.queryEntity(Entity.class, "field1", "field2")
				.executeAndMap(Entity.class)
				.toList()
				.toBlocking()
				.singleOrDefault(null);

			assertNotNull(datas);
			assertTrue(datas.size() > 0);

			System.out.println(datas);
		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	

	@Test
	public void shouldQueryEntitySync() {
		try {
			final List<JsonObject> datas = N1QL.with(bucket)
				.queryEntity(Entity.class, "field1", "field2")
				.execute();

			assertNotNull(datas);
			assertTrue(datas.size() > 0);

			System.out.println(datas);
		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	@Test
	public void shouldQueryEntitySyncAndMap() {
		try {
			final List<Entity> datas = N1QL.with(bucket)
				.queryEntity(Entity.class, "field1", "field2")
				.executeAndMap(Entity.class);

			assertNotNull(datas);
			assertTrue(datas.size() > 0);

			System.out.println(datas);
		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	
	@Test
	public void shouldCreateIndexWithQueryAsync() { //"monIndexSecondaire", "fr.laposte.disf.Toto", "_class");
		
		try {
			N1QL.with(bucket).dropSecondaryIndex("monIndexSecondaire");
		}
		catch (RuntimeException e) {
		}
		
		try {
			N1QL.with(bucket)
				.async()
				.query(simple(Index.createIndex("monIndexSecondaire")
						.on(bucket.name(), x("_class"), x("champ1"), x("champ2"))
						.where(x("_class").eq(s("fr.laposte.disf.Toto")))
				        .using(IndexType.GSI)
						))
				.execute()
				.toBlocking()
				.singleOrDefault(null);

		} 
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	@Before
	public void before() {
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();
		cluster = CouchbaseCluster.create(env, Arrays.asList("10.142.161.101"));
		bucket = cluster.openBucket("beer-sample");
	}

	@After
	public void after() {
		cluster.disconnect();
	}

}
