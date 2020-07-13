package gate.virtualcorpus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;
import org.mapdb.Serializer;

import gate.Document;
import gate.Resource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.serialization.DocumentUtil;

@CreoleResource(name = "MapDbCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by serialized GATE documents in a MapDB")
public class MapDbCorpus extends VirtualCorpus {
	private static final long serialVersionUID = -685151146997248070L;
	private static Logger logger = Logger.getLogger(MapDbCorpus.class);

	protected static final String DOCUMENTSSIZE_MAPNAME = "documentsSize";
	protected static final String DOCUMENTNAMES_MAPNAME = "documentNames";
	protected static final String DOCUMENTBYTES_MAPNAME = "documentBytes";

	private URL mapDbFile;
	private Boolean compressDocuments;

	private transient DB mapDb;
	private transient org.mapdb.Atomic.Integer size;
	private transient Map<Integer, byte[]> documentBytes;
	private transient Map<Integer, String> documentNames;

	@Override
	@Optional
	@CreoleParameter(comment = "If true, documents cannot be added or removed to the corpus", defaultValue = "true")
	public void setImmutableCorpus(Boolean immutableCorpus) {
		super.setImmutableCorpus(immutableCorpus);
	}

	@Override
	public Boolean getImmutableCorpus() {
		return super.getImmutableCorpus();
	}

	@Override
	@Optional
	@CreoleParameter(comment = "If true, changes to content, annotation and feature of documents will not be saved and document names cannot be renamed", defaultValue = "true")
	public final void setReadonlyDocuments(Boolean readonlyDocuments) {
		super.setReadonlyDocuments(readonlyDocuments);
	}

	@Override
	public final Boolean getReadonlyDocuments() {
		return super.getReadonlyDocuments();
	}

	@Optional
	@CreoleParameter(comment = "MapDB file, if empty a tempFileDB is used")
	public void setMapDbFile(URL mapDbFile) {
		this.mapDbFile = mapDbFile;
	}

	public URL getMapDbFile() {
		return mapDbFile;
	}

	@Optional
	@CreoleParameter(comment = "If true, documents will be compressed via deflate", defaultValue = "false")
	public void setCompressDocuments(Boolean compressDocuments) {
		this.compressDocuments = compressDocuments;
	}

	public Boolean getCompressDocuments() {
		return compressDocuments;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		initMapDb();
		initVirtualCorpus();
		return super.init();
	}

	private void initMapDb() throws ResourceInstantiationException {
		Maker maker;
		if (mapDbFile == null) {
			maker = DBMaker.tempFileDB();
		} else {
			File databaseFile;
			try {
				databaseFile = new File(mapDbFile.toURI());
			} catch (URISyntaxException e) {
				throw new ResourceInstantiationException("mapDbFile must be a file", e);
			}
			maker = DBMaker.fileDB(databaseFile);
		}
		mapDb = maker.fileMmapEnableIfSupported().fileMmapPreclearDisable().cleanerHackEnable().fileChannelEnable()
				.make();
		size = mapDb.atomicInteger(DOCUMENTSSIZE_MAPNAME).createOrOpen();
		documentNames = mapDb.hashMap(DOCUMENTNAMES_MAPNAME, Serializer.INTEGER, Serializer.STRING).createOrOpen();
		documentBytes = mapDb.hashMap(DOCUMENTBYTES_MAPNAME, Serializer.INTEGER, Serializer.BYTE_ARRAY).createOrOpen();
	}

	@Override
	public void cleanup() {
		if (mapDb != null) {
			mapDb.close();
		}
	}

	@Override
	protected int loadSize() throws Exception {
		return size.get();
	}

	@Override
	protected String loadDocumentName(int index) throws Exception {
		return documentNames.get(index);
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		if (documentBytes.containsKey(index)) {
			try (InputStream in = new ByteArrayInputStream(documentBytes.get(index))) {
				return DocumentUtil.readDocument(in, compressDocuments);
			}
		}
		return null;
	}

	@Override
	protected void addDocuments(int index, Collection<? extends Document> documents) throws Exception {
		if (index < size()) {
			shiftIndexMap(documentNames, index, size(), documents.size());
			shiftIndexMap(documentBytes, index, size(), documents.size());
		}

		int i = index;
		Iterator<? extends Document> iterator = documents.iterator();
		while (iterator.hasNext()) {
			Document document = iterator.next();
			documentNames.put(i, document.getName());
			documentBytes.put(i, buildBytes(document));
			i++;
		}
		size.addAndGet(documents.size());
	}

	@Override
	protected void setDocument(int index, Document document) throws Exception {
		documentNames.put(index, document.getName());
		documentBytes.put(index, buildBytes(document));
	}

	@Override
	protected void deleteDocuments(Set<Integer> indexes) throws Exception {
		Integer firstIndex = indexes.stream().min(Integer::compareTo).get();
		removeFromIndexMap(documentNames, firstIndex, size(), indexes);
		removeFromIndexMap(documentBytes, firstIndex, size(), indexes);
		size.addAndGet(-indexes.size());
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		documentNames.clear();
		documentBytes.clear();
		size.set(0);
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
		documentNames.put(indexOf(document), newName);
	}

	private byte[] buildBytes(Document document) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			DocumentUtil.writeDocument(document, baos, compressDocuments);
			return baos.toByteArray();
		}
	}

}
