package gate.virtualcorpus;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
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

	public static final String ID_KEY_NAME = "_id";

	private String host;
	private Integer port;
	private String userName;
	private String password;
	private String databaseName;
	private String collectionName;
	private String contentKeys;
	private String featureKeys;

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<org.bson.Document> collection;
	private FindIterable<org.bson.Document> cursor;
	private int cursorPosition;

	private List<String> contentKeyList;
	private List<String> featureKeyList;

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

	@CreoleParameter(comment = "The name of the collection", defaultValue = "")
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	@CreoleParameter(comment = "The name of the content keys (separate multiple values by comma)", defaultValue = "")
	public void setContentKeys(String contentKeys) {
		this.contentKeys = contentKeys;
	}

	public String getContentKeys() {
		return contentKeys;
	}

	@CreoleParameter(comment = "The name of the feature keys (separate multiple values by comma)", defaultValue = "")
	public void setFeatureKeys(String featureKeys) {
		this.featureKeys = featureKeys;
	}

	public String getFeatureKeys() {
		return featureKeys;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType();
		checkValidExporterClassName();
		if (!hasValue(host)) {
			throw new ResourceInstantiationException("host must not be empty");
		}
		if (port == null) {
			throw new ResourceInstantiationException("port must not be empty");
		}
		if (!hasValue(databaseName)) {
			throw new ResourceInstantiationException("databaseName must not be empty");
		}
		if (!hasValue(collectionName)) {
			throw new ResourceInstantiationException("collectionName must not be empty");
		}
		if (!hasValue(contentKeys)) {
			throw new ResourceInstantiationException("contentKeys must not be empty");
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
		collection = database.getCollection(collectionName);
		contentKeyList = splitUserInput(this.contentKeys);
		featureKeyList = splitUserInput(this.featureKeys);

		cursor = collection.find().sort(null);
		cursorPosition = 0;

		initVirtualCorpus();

		return this;
	}

	@Override
	public void cleanup() {
		if (client != null) {
			client.close();
		}
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
		throw new GateRuntimeException("renaming document is not supported");
	}

	@Override
	protected int loadSize() throws Exception {
		long countDocuments = collection.countDocuments();
		if (countDocuments > Integer.MAX_VALUE) {
			throw new IllegalStateException("too many documents in mongodb, unsupported by Gate/List");
		}
		return (int) countDocuments * contentKeyList.size();
	}

	@Override
	protected String loadDocumentName(int index) throws Exception {
		Integer documentIndex = documentIndex(index);
		String contentKey = contentKey(index);

		org.bson.Document document = getDocument(documentIndex);
		Object id = document.get(ID_KEY_NAME);

		return buildDocumentName(id, contentKey);
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		Integer documentIndex = documentIndex(index);
		String contentKey = contentKey(index);

		org.bson.Document mongoDbDocument = getDocument(documentIndex);
		Object content = mongoDbDocument.get(contentKey);
		if (content == null) {
			content = "";
		} else if (content instanceof byte[]) {
			content = new String((byte[]) content, encoding);
		} else if (!(content instanceof String)) {
			content = content.toString();
		}
		FeatureMap features = Factory.newFeatureMap();
		for (String featureKey : featureKeyList) {
			Object feature = mongoDbDocument.get(featureKey);
			features.put(featureKey, feature);
		}
		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		String documentName = buildDocumentName(mongoDbDocument.get(ID_KEY_NAME), contentKey);
		return (Document) Factory.createResource(DocumentImpl.class.getName(), params, features, documentName);
	}

	@Override
	protected Integer addDocuments(int index, Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void setDocument(int index, Document document) throws Exception {
		Integer documentIndex = documentIndex(index);
		String contentKey = contentKey(index);

		org.bson.Document mongoDbDocument = getDocument(documentIndex);

		mongoDbDocument.put(contentKey, export(getExporter(), document));
	}

	@Override
	protected Integer deleteDocuments(Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		throw new UnsupportedOperationException();
	}

	private org.bson.Document getDocument(Integer documentIndex) {
		if (documentIndex != cursorPosition) {
			cursor.skip(documentIndex);
			cursorPosition = documentIndex;
		}
		org.bson.Document document = cursor.first();
		return document;
	}

	private String buildDocumentName(Object id, String contentKey) {
		return id + " " + contentKey;
	}

	private int documentIndex(int index) {
		return index / contentKeyList.size();
	}

	private String contentKey(int index) {
		return contentKeyList.get(index % contentKeyList.size());
	}

}