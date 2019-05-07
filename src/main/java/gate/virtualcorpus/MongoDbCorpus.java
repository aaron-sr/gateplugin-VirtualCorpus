package gate.virtualcorpus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.Resource;
import gate.corpora.DocumentImpl;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.util.GateRuntimeException;

@CreoleResource(name = "MongoDbCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents stored in a MongoDB")
public class MongoDbCorpus extends VirtualCorpus implements Corpus {
	private static final long serialVersionUID = -4207705811952799973L;
	private static Logger logger = Logger.getLogger(MongoDbCorpus.class);

	public static final String ID_KEY_NAME = "_id";

	protected String host;
	protected Integer port;
	protected String userName;
	protected String password;
	protected String databaseName;
	protected String collectionName;
	protected String contentKeys;
	protected String featureKeys;
	protected String exportKeySuffix;
	protected String exportEncoding;
	protected Integer batchSize;
	protected Boolean cacheIds;

	private List<String> contentKeyList;
	private List<String> featureKeyList;
	private Map<String, String> exportKeyMapping;

	private MongoClient client;
	private MongoDatabase database;
	private MongoCollection<org.bson.Document> collection;
	private FindIterable<org.bson.Document> cursor;
	private int cursorPosition;
	private Map<Integer, String> idCache;
	private MongoCursor<org.bson.Document> iterator;
	private int iteratorPosition;
	private DB cacheDb;

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

	@Optional
	@CreoleParameter(comment = "suffix for value keys, where exported data is written", defaultValue = "")
	public void setExportKeySuffix(String exportKeySuffix) {
		this.exportKeySuffix = exportKeySuffix;
	}

	public String getExportKeySuffix() {
		return exportKeySuffix;
	}

	@Optional
	@CreoleParameter(comment = "encoding for value keys, which were exported (in case of reopen document)", defaultValue = "")
	public void setExportEncoding(String exportEncoding) {
		this.exportEncoding = exportEncoding;
	}

	public String getExportEncoding() {
		return exportEncoding;
	}

	@Optional
	@CreoleParameter(comment = "batch size of mongodb cursor", defaultValue = "")
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	@Optional
	@CreoleParameter(comment = "cache ids to reduce cursor skip", defaultValue = "false")
	public void setCacheIds(Boolean cacheIds) {
		this.cacheIds = cacheIds;
	}

	public Boolean getCacheIds() {
		return cacheIds;
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
		if (hasValue(exportKeySuffix)) {
			exportKeyMapping = new HashMap<>();
			for (String contentKey : contentKeyList) {
				String exportKey = contentKey + exportKeySuffix;
				exportKeyMapping.put(contentKey, exportKey);
			}
		}

		cursor = collection.find().noCursorTimeout(true).sort(Sorts.ascending(ID_KEY_NAME));
		if (batchSize != null) {
			cursor = cursor.batchSize(batchSize);
		}
		cursorPosition = 0;

		if (cacheIds) {
			cursor = cursor.projection(Projections.include(ID_KEY_NAME));
			iterator = cursor.iterator();
			cacheDb = DBMaker.tempFileDB().fileMmapEnableIfSupported().fileMmapPreclearDisable().cleanerHackEnable()
					.fileChannelEnable().make();
			idCache = cacheDb.hashMap(name, Serializer.INTEGER, Serializer.STRING).createOrOpen();
		}

		initVirtualCorpus();

		return this;
	}

	@Override
	public void cleanup() {
		if (iterator != null) {
			iterator.close();
		}
		if (client != null) {
			client.close();
		}
		if (cacheDb != null) {
			cacheDb.close();
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

		Object id = getId(documentIndex);

		return buildDocumentName(id, contentKey);
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		Integer documentIndex = documentIndex(index);
		String contentKey = contentKey(index);

		List<String> includeKeys = new ArrayList<>();
		includeKeys.add(contentKey);
		includeKeys.addAll(featureKeyList);
		if (hasValue(exportKeySuffix)) {
			String exportKey = exportKeyMapping.get(contentKey);
			includeKeys.add(exportKey);
		}
		org.bson.Document mongoDbDocument = getDocument(documentIndex, includeKeys);

		Object content = null;
		String encoding = null;
		String mimeType = null;
		if (hasValue(exportKeySuffix)) {
			String exportKey = exportKeyMapping.get(contentKey);
			content = mongoDbDocument.get(exportKey);
			encoding = exportEncoding;
			mimeType = getExporter().getMimeType();
		}
		if (content == null) {
			content = mongoDbDocument.get(contentKey);
			encoding = this.encoding;
			mimeType = this.mimeType;
		}
		if (content == null) {
			content = "";
		} else if (content instanceof org.bson.types.Binary) {
			content = new String(((org.bson.types.Binary) content).getData(), encoding);
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

		if (hasValue(exportKeySuffix)) {
			contentKey = exportKeyMapping.get(contentKey);
		}
		byte[] bytes = export(getExporter(), document);

		String id = getId(documentIndex);

		collection.updateOne(Filters.eq(ID_KEY_NAME, new ObjectId(id)), Updates.set(contentKey, bytes));
	}

	@Override
	protected Integer deleteDocuments(Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		throw new UnsupportedOperationException();
	}

	private String getId(Integer documentIndex) {
		if (cacheIds) {
			String id = idCache.get(documentIndex);
			if (id != null) {
				return id;
			}
			while (iteratorPosition <= documentIndex) {
				org.bson.Document mongoDbDocument = iterator.next();
				String next = mongoDbDocument.getObjectId(ID_KEY_NAME).toHexString();
				idCache.put(iteratorPosition, next);
				try {
					if (iteratorPosition == documentIndex) {
						return next;
					}
				} finally {
					iteratorPosition++;
				}
			}
		}
		return getDocument(documentIndex, Collections.emptyList()).getObjectId(ID_KEY_NAME).toHexString();
	}

	private org.bson.Document getDocument(Integer documentIndex, List<String> includeKeys) {
		if (cacheIds) {
			String id = getId(documentIndex);
			return collection.find(Filters.eq(ID_KEY_NAME, new ObjectId(id)))
					.projection(Projections.include(includeKeys)).first();
		}
		if (documentIndex != cursorPosition) {
			cursor.skip(documentIndex);
			cursorPosition = documentIndex;
		}
		return cursor.projection(Projections.include(includeKeys)).first();
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