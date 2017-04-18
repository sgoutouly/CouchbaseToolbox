package com.sylvaingoutouly.cb;

import java.util.List;

import rx.Observable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.jayway.jsonpath.JsonPath;

/**
 * 
 * @author sylvain
 */
public class Joint {

	static class JointBuilder implements JointFrom, JointTo, JointRun {
		
		private Bucket bucket;
		private String[] fromKey;
		private String jsonPathTo;

		public JointBuilder(Bucket bucket) {
			this.bucket = bucket;
		}

		@SuppressWarnings("unchecked")
		public Observable<JsonDocument> execute(boolean includeFrom) {
			
			final Observable<JsonDocument> fromDoc = Observable
					.from(fromKey)
					.flatMap(id -> bucket.async().get(id));
			
			final Observable<JsonDocument> foreigns = fromDoc
				.switchIfEmpty(Observable.<JsonDocument>error(new IllegalStateException("Document not found !")))
				.map(doc -> doc.content().toString())
				.map(json -> JsonPath.parse(json).read("$." + jsonPathTo ))
				.flatMap(o -> o instanceof String ? Observable.just((String) o) : Observable.from((List<String>) o))
				.flatMap(id -> bucket.async().get(id));
			
			return includeFrom ? foreigns.mergeWith(fromDoc) : foreigns;
		}

		public JointRun to(String jsonPathTo) {
			this.jsonPathTo = jsonPathTo;
			return this;
		}

		public JointTo from(String... key) {
			this.fromKey = key;
			return this;
		}

	}

	public static JointFrom with(Bucket bucket) {
		return new JointBuilder(bucket);
	}

	interface JointFrom { JointTo from(String... key);	}

	interface JointTo {	JointRun to(String jsonPathTo);	}

	interface JointRun { Observable<JsonDocument> execute(boolean addFromDoc); }
	
}
