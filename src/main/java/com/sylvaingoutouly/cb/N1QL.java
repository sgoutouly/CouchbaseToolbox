package com.sylvaingoutouly.cb;

import static com.couchbase.client.java.query.Index.createNamedPrimaryIndex;
import static com.couchbase.client.java.query.Index.dropNamedPrimaryIndex;
import static com.couchbase.client.java.query.N1qlQuery.simple;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static java.util.Arrays.stream;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import rx.Observable;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.index.IndexType;

/**
 * N1QL helper
 * Permet d'effectuer différents types de requêtes N1QL de manière asynchrone tout en 
 * pouvant récupérer les résultats sous forme d'observables ou directement sous forme de liste 
 * (le système bloque pour vous).
 * Propose des modes de requêtages classiques ou basés sur des entités. 
 * Assure le positionement du nom de bucket dans les requêtes textuelle si le palceholder 
 * %bucket% est utilisé.
 * Permet d'effectuer des opération sur les index primaires et secondaires : création, suppression, 
 * index secondaire couvrant 
 *  
 * @author sylvain
 */
public class N1QL {
	
	private static final ObjectMapper mapper = new ObjectMapper();

	/**
	 * Le builder à étape permettant de produire les requêteurs
	 * @author sylvain
	 */
	@RequiredArgsConstructor
	static class N1QLBuilder implements SyncQuery {
		
		private final Bucket bucket;
		private N1qlQuery query;
	
		// ------- Sync operations
		
		@Override
		public List<JsonObject> execute() {
			return async().execute()
				.toList()
				.toBlocking()
				.lastOrDefault(null);
		}

		@Override
		public <T> List<T> executeAndMap(Class<T> entity) {
			return async().executeAndMap(entity)
					.toList()
					.toBlocking()
					.lastOrDefault(null);
		}
		
		@Override
		public SyncQuery query(String query) {
			final String q = query.replaceAll("%bucket%", "`" + this.bucket.name() + "`");
			this.query = N1qlQuery.simple(q);
			return this;
		}
		
		@Override
		public SyncQuery query(N1qlQuery query) {
			this.query = query;
			return this;
		}

		@Override
		public SyncQuery queryEntity(Class<?> entity, String... fields) {
			final String q = fieldsArrayToString(fields);
			return query("select " + q + " from %bucket% where `_class` =\"" + entity.getName() + "\"");
		}
		
		// ------- Async operations
		
		private static <T> T jsonToObject(JsonObject json, Class<T> entity) {
			try{
				return mapper.readValue(json.toString(), entity);
			}
			catch (IOException e) {
				throw new RuntimeException(e.getMessage());
			}
		}
		
		public AsyncQuery async() {
			return new AsyncQuery() {
				@Override
				public <T> Observable<T> executeAndMap(Class<T> entity) {
					return execute().map(json -> jsonToObject(json, entity));
				}
				
				@Override
				public Observable<JsonObject> execute() {
					return bucket.async()
						.query(N1QLBuilder.this.query)
						.flatMap(AsyncN1qlQueryResult::rows)
						.map(AsyncN1qlQueryRow::value);
				}

				@Override
				public AsyncQuery query(String query) {
					final String q = query.replaceAll("%bucket%", "`" + N1QLBuilder.this.bucket.name() + "`");
					System.out.println(q);
					N1QLBuilder.this.query = N1qlQuery.simple(q);
					return this;
				}

				@Override
				public AsyncQuery query(N1qlQuery query) {
					N1QLBuilder.this.query = query;
					return this;
				}

				@Override
				public AsyncQuery queryEntity(Class<?> entity, String... fields) {
					final String q = "SELECT " + fieldsArrayToString(fields) + " FROM %bucket% WHERE `_class`=\"" + entity.getName() + "\"";
					return query(q);
				}

			};
		}
		
		
		// ------- Index management
		
		@Override 
		public void dropPrimaryIndex(String idxName) {
			final String bucket = N1QLBuilder.this.bucket.name();
			N1qlQueryResult r;
			if (idxName == null) {
				r = N1QLBuilder.this.bucket
					.query(simple(Index.dropPrimaryIndex(bucket).using(IndexType.GSI)));
			}
			else {
				r = N1QLBuilder.this.bucket.query(
						simple(dropNamedPrimaryIndex(bucket, idxName).using(IndexType.GSI)));
			}
			
			if (!r.finalSuccess()) {
				throw new RuntimeException(r.errors().toString());
			}
		}
		
		@Override 
		public void dropPrimaryIndex() {
			dropPrimaryIndex(null);
		}
		
		@Override 
		public void createPrimaryIndex(String name) {
			final String bucket = N1QLBuilder.this.bucket.name();
			N1qlQueryResult r;
			if (name == null) {
				r = N1QLBuilder.this.bucket.query(
						simple(Index.createPrimaryIndex().on(bucket).using(IndexType.GSI)));
			}
			else {
				r = N1QLBuilder.this.bucket.query(
						simple(createNamedPrimaryIndex(name).on(bucket).using(IndexType.GSI)));
			}
			if (!r.finalSuccess()) {
				throw new RuntimeException(r.errors().toString());
			}
		}
		
		@Override 
		public void createPrimaryIndex() {
			createPrimaryIndex(null);
		}
		
		@Override
		public void createSecondaryIndex(String idxName, String filter, String field, String...additionalFields) {
			final Expression[] exp = stream(additionalFields)
					.map(v -> x(v))
					.toArray(size -> new Expression[size]);
			
			final String bucket = N1QLBuilder.this.bucket.name();
			
			final N1qlQueryResult r = this.bucket.query(
				simple(Index.createIndex(idxName)
					.on(bucket, x(field), exp)
					.where(x(field).eq(s(filter)))
			        .using(IndexType.GSI)
					));
			
			if (!r.finalSuccess()) {
				throw new RuntimeException(r.errors().toString());
			}
		}

		@Override
		public void dropSecondaryIndex(String idxName) {
			final String bucket = N1QLBuilder.this.bucket.name();
			final N1qlQueryResult r = this.bucket.query(simple(Index.dropIndex(bucket, idxName)));
			
			if (!r.finalSuccess()) {
				throw new RuntimeException(r.errors().toString());
			}
		}

	}
	
	/** 
	 * Crée l'instance de builder à partir du bucket à manipuler
	 * @param bucket Le {@link Bucket} préalablement configuré 
	 */
	public static SyncQuery with(Bucket bucket) { 
		return new N1QLBuilder(bucket); 
	}
	
	/**
	 * Opérations N1QL synchrones
	 */
	interface SyncQuery {
		
		/**
		 * Créé un index primaire avec le nom par défaut (#primary)
		 */
		void createPrimaryIndex();
		
		/**
		 * Créé un index primaire en précisant son nom
		 * @param idxName le nom de l'index
		 */
		void createPrimaryIndex(String idxName);
		
		/**
		 * Supprime l'index primaire par défaut (#primary)
		 */
		void dropPrimaryIndex();
		
		/**
		 * Supprime un index primaire
		 * @param idxName Le nom de l'index à supprimer
		 */
		void dropPrimaryIndex(String idxName);
		
		/**
		 * Créé un index secondaire couvrant (incluant les champs devant être renvoyés par la requête et 
		 * pas seulement celui sur lequel s'applique le filtre)
		 * 
		 * @param idxName Nom de l'index
		 * @param filter Filtre de sélection des documents à inclure dans l'index (valeur attendue dans le champ)
		 * @param field Nom du champ sur lequel on applique le filtre
		 * @param additionalFields Champs additionnels à placer dans l'index
		 * 
		 * <br/>L'appel :
		 * <code> createSecondaryIndex("monIndexSecondaireCouvrant", "com.acme.Foo", "_class", "champSupp1", "champSupp2")</code>
		 * génèrera l'index suivant :</br>
		 * <code>CREATE INDEX `monIndexSecondaireCouvrant` ON `beer-sample`(`_class`,`champSupp1`,`champSupp2`) WHERE (`_class` = "com.acme.Foo")</code>
		 */
		void createSecondaryIndex(String idxName, String filter, String field, String...additionalFields);
		
		/**
		 * Supprime un index secondaire
		 * @param idxName Le nom de l'index à supprimer
		 */
		void dropSecondaryIndex(String idxName);
		
		/**
		 * Prépare une requête select dont le filtre est l'entité java passée en argument. 
		 * La liste des champs renvoyés est configurable. Ceci est utile pour créer une liste d'entités persistées
		 * par Spring Data Couchbase en utilisant un subset des champs afin de profiter d'un index couvrant et ainsi
		 * bénéficier d'un maximum de performance.
		 * 
		 * @param entity La classe de mapping utilisées pour créer les documents (cf. Spring Data Couchbase)
		 * @param fields les attributs à renvoyer par la requête
		 * @return {@link SyncQuery} Le requêteur contenant la requête prête à exécuter
		 */
		SyncQuery queryEntity(Class<?> entity, String... fields);

		/**
		 * Prépare une requête N1ql classique
		 * 
		 * @param query La requête de type {@link N1qlQuery}
		 * @return {@link SyncQuery} Le requêteur contenant la requête prête à exécuter
		 */
		SyncQuery query(N1qlQuery query); 
		
		/**
		 * Prépare une requête N1ql classique à partir d'une chaîne. Si le placehoder %bucket% est trouvé 
		 * dans la chaîne, il sera remplacé par le nom du bucket correctement "échappé" 
		 * 
		 * @param query La requête de type String
		 * @return {@link SyncQuery} Le requêteur contenant la requête prête à exécuter
		 */
		SyncQuery query(String query);
		
		/**
		 * Bascule le requêteur en mode asynchrone
		 * @return {@link AsyncQuery} Une instance requêteur asynchrone
		 */
		AsyncQuery async(); 
		
		/**
		 * Exécute la requête et renvoie les résultats en mode synchrone
		 * @return List<JsonObject> la liste des résulats au format {@link JsonObject}
		 */
		List<JsonObject> execute(); 
		
		/**
		 * Exécute la requête et renvoie les résultats en synchrone après avoir transformé
		 * les JsonObject en entité Java
		 * @return List<T> Une liste des résulats au format T
		 */
		<T> List<T> executeAndMap(Class<T> entity);
	}
	
	/**
	 * Opérations N1QL asynchrones
	 */
	interface AsyncQuery {
		
		/**
		 * Prépare une requête select dont le filtre est l'entité java passée en argument. 
		 * La liste des champs renvoyés est configurable. Ceci est utile pour créer une liste d'entités persistées
		 * par Spring Data Couchbase en utilisant un subset des champs afin de profiter d'un index couvrant et ainsi
		 * bénéficier d'un maximum de performance.
		 * 
		 * @param entity La classe de mapping utilisées pour créer les documents (cf. Spring Data Couchbase)
		 * @param fields les attributs à renvoyer par la requête
		 * @return {@link AsyncQuery} Le requêteur contenant la requête prête à exécuter
		 */
		AsyncQuery queryEntity(Class<?> entity, String... fields);
		
		/**
		 * Prépare une requête N1ql classique
		 * 
		 * @param query La requête de type {@link N1qlQuery}
		 * @return {@link AsyncQuery} Le requêteur asynchrone contenant la requête prête à exécuter
		 */
		AsyncQuery query(N1qlQuery query); 
		
		/**
		 * Prépare une requête N1ql classique à partir d'une chaîne
		 * 
		 * @param query La requête de type String
		 * @return {@link AsyncQuery} Le requêteur asynchrone contenant la requête prête à exécuter
		 */
		AsyncQuery query(String query); 
		
		/**
		 * Exécute la requête et renvoie les résultats en mode asynchrone
		 * @return Observable<JsonObject> Un Observable de la liste des résulats au format {@link JsonObject}
		 */
		Observable<JsonObject> execute(); 
		
		/**
		 * Exécute la requête et renvoie les résultats en mode asynchrone après avoir transformé
		 * les JsonObject en entité Java
		 * @return Observable<T> Un Observable de la liste des résulats au format T
		 */
		<T> Observable<T> executeAndMap(Class<T> entity); 
		
	}
	
	
	// --- utilities
	
	private static String fieldsArrayToString(String ... fields) {
		return stream(fields).collect(Collectors.joining(","));
	}
	
}
