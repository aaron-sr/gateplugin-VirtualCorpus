package gate.virtualcorpus;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.GateConstants;
import gate.Resource;
import gate.corpora.DocumentImpl;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.serialization.DocumentUtil;

@CreoleResource(name = "SerializedFilesCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents serialized in files in a single directory")
public class SerializedFilesCorpus extends VirtualCorpus {
	private static final long serialVersionUID = 2056672632092000437L;
	private static Logger logger = Logger.getLogger(SerializedFilesCorpus.class);

	public static final String SERIALIZED_FILE_EXTENSION = ".ser";
	public static final String COMPRESSED_FILE_EXTENSION = ".zz";

	protected URL directoryURL;
	protected Boolean compressFiles;
	protected String encoding;
	protected String mimeType;

	private transient Path directory;
	private transient Integer size;
	private transient boolean regularFiles;
	private transient List<Path> paths;

	@CreoleParameter(comment = "The directory URL where files will be read from", defaultValue = "")
	public void setDirectoryURL(URL directoryURL) {
		this.directoryURL = directoryURL;
	}

	public URL getDirectoryURL() {
		return directoryURL;
	}

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
	@CreoleParameter(comment = "If true, document files will be compressed via deflate", defaultValue = "false")
	public void setCompressFiles(Boolean compressFiles) {
		this.compressFiles = compressFiles;
	}

	public Boolean getCompressFiles() {
		return compressFiles;
	}

	@Optional
	@CreoleParameter(comment = "encoding to read and write document content", defaultValue = "")
	public final void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public final String getEncoding() {
		return encoding;
	}

	@Optional
	@CreoleParameter(comment = "mimeType to read and write document content", defaultValue = "")
	public final void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	public final String getMimeType() {
		return mimeType;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType(mimeType, false);
		if (directoryURL == null) {
			throw new ResourceInstantiationException("directoryURL must be set");
		}
		try {
			directory = gate.util.Files.fileFromURL(directoryURL).toPath();
		} catch (Exception e) {
			throw new ResourceInstantiationException("directoryURL is not a valid file url", e);
		}
		if (!Files.exists(directory)) {
			try {
				Files.createDirectories(directory);
			} catch (IOException e) {
				throw new ResourceInstantiationException(e);
			}
		}
		if (!Files.isDirectory(directory)) {
			throw new ResourceInstantiationException("directoryURL is not a directory");
		}
		try {
			if (containsDirectories(directory)) {
				throw new ResourceInstantiationException("directory contains sub directories");
			}

			regularFiles = false;
			int maxIndex = -1;
			Iterator<Path> iterator = Files.list(directory).iterator();
			while (iterator.hasNext()) {
				Path path = iterator.next();
				int index = getIndex(path);
				if (index < 0) {
					regularFiles = true;
					break;
				} else {
					maxIndex = Math.max(maxIndex, index);
				}
			}

			if (regularFiles) {
				try (Stream<Path> stream = Files.list(directory)) {
					paths = stream.collect(Collectors.toList());
				}
				paths.removeAll(paths.stream().map(path -> writePath(path)).collect(Collectors.toSet()));
				size = paths.size();
			} else {
				size = maxIndex + 1;
			}

		} catch (IOException e) {
			throw new ResourceInstantiationException(e);
		}

		initVirtualCorpus();

		return this;
	}

	@Override
	protected int loadSize() throws Exception {
		return size;
	}

	@Override
	protected String loadDocumentName(int index) throws Exception {
		if (regularFiles) {
			Path path = paths.get(index);
			return path.getFileName().toString();
		} else {
			Path path = indexedPath(index);
			if (!Files.exists(path)) {
				return null;
			}
			return DocumentUtil.readDocumentName(Files.newInputStream(path), compressFiles);
		}
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		if (regularFiles) {
			Path path = paths.get(index);
			Path writePath = writePath(path);
			if (Files.exists(writePath)) {
				return loadDocument(writePath);
			}
			String documentName = getDocumentName(index);
			String content = new String(Files.readAllBytes(path));
			FeatureMap features = Factory.newFeatureMap();
			features.put(GateConstants.THROWEX_FORMAT_PROPERTY_NAME, true);
			FeatureMap params = Factory.newFeatureMap();
			params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
			params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
			params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
			return (Document) Factory.createResource(DocumentImpl.class.getName(), params, features, documentName);
		} else {
			Path path = indexedPath(index);
			if (!Files.exists(path)) {
				return null;
			}
			return loadDocument(path);
		}

	}

	@Override
	protected void addDocuments(int index, Collection<? extends Document> documents) throws Exception {
		if (regularFiles) {
			throw new UnsupportedOperationException();
		}
		int insertCount = documents.size();
		for (int i = size() - 1; i >= index; i--) {
			Path oldPath = indexedPath(i);
			Path newPath = indexedPath(i + insertCount);
			Files.move(oldPath, newPath);
		}
		Iterator<? extends Document> iterator = documents.iterator();
		for (int i = index; i < index + insertCount; i++) {
			setDocument(i, iterator.next());
		}
	}

	@Override
	protected void setDocument(int index, Document document) throws Exception {
		Path path;
		if (regularFiles) {
			path = writePath(paths.get(index));
		} else {
			path = indexedPath(index);
		}
		DocumentUtil.writeDocument(document, Files.newOutputStream(path), compressFiles);
	}

	@Override
	protected void deleteDocuments(Set<Integer> indexes) throws Exception {
		if (regularFiles) {
			throw new UnsupportedOperationException();
		}
		Integer firstIndex = indexes.stream().min(Integer::compareTo).get();
		Integer lastIndex = size();

		Integer newIndex = firstIndex;
		for (Integer index = firstIndex; index <= lastIndex; index++) {
			if (!indexes.contains(index)) {
				Files.move(indexedPath(index), indexedPath(newIndex++));
			}
		}
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		try (Stream<Path> stream = Files.list(directory)) {
			stream.forEach(path -> {
				try {
					Files.delete(path);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
		if (regularFiles) {
			throw new UnsupportedOperationException();
		}
		document.setName(newName);
		setDocument(this.indexOf(document), document);
	}

	private Document loadDocument(Path path) throws Exception {
		return DocumentUtil.readDocument(Files.newInputStream(path), compressFiles);
	}

	private Path indexedPath(int index) {
		String filename = String.valueOf(index);
		return directory.resolve(writePath(Paths.get(filename)));
	}

	private int getIndex(Path path) {
		String filename = path.getFileName().toString();
		String extension = getWriteExtension();
		if (filename.endsWith(extension)) {
			filename = filename.substring(0, filename.length() - extension.length());
		}

		try {
			return Integer.valueOf(filename);
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	private Path writePath(Path path) {
		String extension = getWriteExtension();
		return path.resolveSibling(path.getFileName() + extension);
	}

	private String getWriteExtension() {
		String extension = SERIALIZED_FILE_EXTENSION;
		if (compressFiles) {
			extension += COMPRESSED_FILE_EXTENSION;
		}
		return extension;
	}

	private static boolean containsDirectories(final Path directory) throws IOException {
		try (Stream<Path> stream = Files.list(directory)) {
			return stream.anyMatch(path -> Files.isDirectory(path));
		}
	}

}
