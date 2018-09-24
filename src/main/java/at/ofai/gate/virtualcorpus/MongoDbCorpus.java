package at.ofai.gate.virtualcorpus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.Resource;
import gate.corpora.DocumentImpl;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.util.GateRuntimeException;

@CreoleResource(name = "MongoDbCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents stored in a MongoDB")
public class MongoDbCorpus extends VirtualCorpus implements Corpus {
	private static final long serialVersionUID = -4207705811952799973L;
	private static Logger logger = Logger.getLogger(MongoDbCorpus.class);

	public static final String FEATURE_MONGODB_COLLECTION_NAME = "mongoDbCollection";
	public static final String FEATURE_MONGODB_DOCUMENT_ID = "mongoDbDocument";
	public static final String FEATURE_MONGODB_KEY_NAME = "mongoDbKey";

	public static final String MONGODB_ID_KEY_NAME = "_id";

	private String host;
	private Integer port;
	private String userName;
	private String password;
	private String databaseName;
	private String collectionName;
	private String keyName;

	private Map<String, Map<String, Object>> documentFeatures = new HashMap<>();
	private MongoClient client;
	private MongoDatabase database;

	@CreoleParameter(comment = "The host of the MongoDB", defaultValue = "localhost")
	public void setHost(String host) {
		this.host = host;
	}

	public String getHost() {
		return host;
	}

	@CreoleParameter(comment = "The port of the MongoDB", defaultValue = "27017")
	public void setPort(Integer port) {
		this.port = port;
	}

	public Integer getPort() {
		return port;
	}

	@CreoleParameter(comment = "The username of the MongoDB (leave empty if no authentification)", defaultValue = "")
	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserName() {
		return userName;
	}

	@CreoleParameter(comment = "The password of the MongoDB (leave empty if no authentification)", defaultValue = "")
	public void setPassword(String password) {
		this.password = password;
	}

	public String getPassword() {
		return password;
	}

	@CreoleParameter(comment = "The name of the database", defaultValue = "")
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	@CreoleParameter(comment = "The name of the collection (separate multiple values by comma, leave empty for all collections)", defaultValue = "")
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	@CreoleParameter(comment = "The name of the key (separate multiple values by comma, leave empty for all keys)", defaultValue = "")
	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getKeyName() {
		return keyName;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType();
		if (!hasValue(host)) {
			throw new ResourceInstantiationException("host must not be empty");
		}
		if (port == null) {
			throw new ResourceInstantiationException("port must not be empty");
		}
		if (!hasValue(databaseName)) {
			throw new ResourceInstantiationException("databaseName must not be empty");
		}

		ServerAddress serverAddress = new ServerAddress(host, port);
		MongoClientOptions options = MongoClientOptions.builder().build();

		if (hasValue(userName) && hasValue(password)) {
			MongoCredential credential = MongoCredential.createCredential(userName, databaseName,
					password.toCharArray());
			client = new MongoClient(serverAddress, credential, options);
		} else {
			client = new MongoClient(serverAddress, options);
		}
		database = client.getDatabase(databaseName);

		List<String> collectionNames = new ArrayList<>();
		if (hasValue(collectionName)) {
			for (String name : collectionName.split(",")) {
				collectionNames.add(name.trim());
			}
		} else {
			database.listCollectionNames().into(collectionNames);
		}
		List<String> keyNames = new ArrayList<>();
		if (hasValue(keyName)) {
			for (String name : keyName.split(",")) {
				keyNames.add(name.trim());
			}
		} else {
			keyNames = null;
		}

		List<String> documentNames = new ArrayList<>();
		for (String collectionName : collectionNames) {
			MongoCollection<org.bson.Document> collection = database.getCollection(collectionName);
			MongoCursor<org.bson.Document> iterator = collection.find().iterator();
			while (iterator.hasNext()) {
				org.bson.Document document = iterator.next();
				Object id = document.get(MONGODB_ID_KEY_NAME);
				Collection<String> keys = keyNames != null ? keyNames : document.keySet();
				for (String key : keys) {
					if (document.containsKey(key) && !key.contentEquals(MONGODB_ID_KEY_NAME)) {
						String documentName = collectionName + "#" + id + "~" + key;
						Map<String, Object> features = new HashMap<>();
						features.put(FEATURE_MONGODB_COLLECTION_NAME, collectionName);
						features.put(FEATURE_MONGODB_DOCUMENT_ID, id);
						features.put(FEATURE_MONGODB_KEY_NAME, key);
						documentFeatures.put(documentName, features);
						documentNames.add(documentName);
					}
				}
			}
		}

		initVirtualCorpus(documentNames);

		return this;
	}

	@Override
	public void cleanup() {
		if (client != null) {
			client.close();
		}
	}

	@Override
	protected Document readDocument(String documentName) throws Exception {
		Map<String, Object> features = documentFeatures.get(documentName);
		String collectionName = (String) features.get(FEATURE_MONGODB_COLLECTION_NAME);
		String id = (String) features.get(FEATURE_MONGODB_DOCUMENT_ID);
		String key = (String) features.get(FEATURE_MONGODB_KEY_NAME);

		String content = database.getCollection(collectionName).find(queryById(id)).first().get(key).toString();

		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content != null ? content : "");
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		Document document = (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, documentName);
		document.getFeatures().putAll(features);
		return document;
	}

	@Override
	protected void createDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		String collectionName = (String) features.get(FEATURE_MONGODB_COLLECTION_NAME);
		Object id = features.get(FEATURE_MONGODB_DOCUMENT_ID);
		String key = (String) features.get(FEATURE_MONGODB_KEY_NAME);

		MongoCollection<org.bson.Document> collection = database.getCollection(collectionName);
		org.bson.Document mongoDocument = collection.find(queryById(id)).first();
		if (mongoDocument != null) {
			mongoDocument.put(key, export(getExporter(mimeType), document));
		} else {
			mongoDocument = new org.bson.Document();
			mongoDocument.put(key, export(getExporter(mimeType), document));
			collection.insertOne(mongoDocument);
			id = mongoDocument.getObjectId(MONGODB_ID_KEY_NAME);
			features.put(FEATURE_MONGODB_DOCUMENT_ID, id);
		}
	}

	@Override
	protected void updateDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		String collectionName = (String) features.get(FEATURE_MONGODB_COLLECTION_NAME);
		Object id = features.get(FEATURE_MONGODB_DOCUMENT_ID);
		String key = (String) features.get(FEATURE_MONGODB_KEY_NAME);

		database.getCollection(collectionName).find(queryById(id)).first().put(key,
				export(getExporter(mimeType), document));
	}

	@Override
	protected void deleteDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		String collectionName = (String) features.get(FEATURE_MONGODB_COLLECTION_NAME);
		Object id = features.get(FEATURE_MONGODB_DOCUMENT_ID);
		String key = (String) features.get(FEATURE_MONGODB_KEY_NAME);

		database.getCollection(collectionName).find(queryById(id)).first().remove(key);
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
		throw new GateRuntimeException("renaming document is not supported");
	}

	private Bson queryById(Object id) {
		BasicDBObject query = new BasicDBObject();
		query.put(MONGODB_ID_KEY_NAME, id);
		return query;
	}

}