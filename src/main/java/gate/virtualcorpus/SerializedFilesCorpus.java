package gate.virtualcorpus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;

import org.apache.log4j.Logger;

import gate.Document;
import gate.Gate;
import gate.Resource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;

@CreoleResource(name = "SerializedFilesCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents serialized in files in a single directory")
public class SerializedFilesCorpus extends VirtualCorpus {
	private static final long serialVersionUID = 2056672632092000437L;
	private static Logger logger = Logger.getLogger(SerializedFilesCorpus.class);

	public static final String FILE_EXTENSION = ".ser";

	protected URL directoryURL;
	protected Boolean compressedFiles;

	private File directory;

	@CreoleParameter(comment = "The directory URL where files will be read from", defaultValue = "")
	public void setDirectoryURL(URL directoryURL) {
		this.directoryURL = directoryURL;
	}

	public URL getDirectoryURL() {
		return directoryURL;
	}

	@Optional
	@CreoleParameter(comment = "If true, documents cannot be added or removed to the corpus", defaultValue = "true")
	public void setImmutableCorpus(Boolean immutableCorpus) {
		this.immutableCorpus = immutableCorpus;
	}

	public Boolean getImmutableCorpus() {
		return immutableCorpus;
	}

	@Optional
	@CreoleParameter(comment = "If true, changes to content, annotation and feature of documents will not be saved and document names cannot be renamed", defaultValue = "true")
	public final void setReadonlyDocuments(Boolean readonlyDocuments) {
		this.readonlyDocuments = readonlyDocuments;
	}

	public final Boolean getReadonlyDocuments() {
		return this.readonlyDocuments;
	}

	@Optional
	@CreoleParameter(comment = "If true, document files will be compressed via deflate", defaultValue = "false")
	public void setCompressedFiles(Boolean compressedFiles) {
		this.compressedFiles = compressedFiles;
	}

	public Boolean getCompressedFiles() {
		return compressedFiles;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		if (directoryURL == null) {
			throw new ResourceInstantiationException("directoryURL must be set");
		}
		try {
			directory = gate.util.Files.fileFromURL(directoryURL).getCanonicalFile();
		} catch (Exception e) {
			throw new ResourceInstantiationException("directoryURL is not a valid file url", e);
		}
		if (!directory.exists()) {
			directory.mkdirs();
		}
		if (!directory.isDirectory()) {
			throw new ResourceInstantiationException("directoryURL is not a directory");
		}

		initVirtualCorpus();

		return this;
	}

	@Override
	protected int loadSize() throws Exception {
		try (Stream<Path> stream = Files.list(Paths.get(directoryURL.toURI()))
				.filter(p -> p.getFileName().toString().endsWith(FILE_EXTENSION))) {
			return (int) stream.count();
		}
	}

	@Override
	protected String loadDocumentName(int index) throws Exception {
		Document document = loadDocument(index);
		documentLoaded(index, document);
		return document.getName();
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		try (ObjectInputStream inputStream = new GateObjectInputStream(openInputStream(index))) {
			return (Document) inputStream.readObject();
		}
	}

	@Override
	protected void addDocuments(int index, Collection<? extends Document> documents) throws Exception {
		int insertCount = documents.size();
		for (int i = size() - 1; i >= index; i--) {
			File oldFile = getFile(i);
			File newFile = getFile(i + insertCount);
			boolean success = oldFile.renameTo(newFile);
			if (!success) {
				throw new IllegalStateException("cannot rename file '" + oldFile.getAbsolutePath() + "' to '"
						+ newFile.getAbsolutePath() + "'");
			}
		}
		Iterator<? extends Document> iterator = documents.iterator();
		for (int i = index; i < index + insertCount; i++) {
			setDocument(i, iterator.next());
		}
	}

	@Override
	protected void setDocument(int index, Document document) throws Exception {
		try (ObjectOutputStream outputStream = new ObjectOutputStream(openOutputStream(index))) {
			outputStream.writeObject(document);
			outputStream.flush();
		}
	}

	@Override
	protected void deleteDocuments(Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		try (Stream<Path> stream = Files.list(Paths.get(directory.toURI()))
				.filter(p -> p.getFileName().endsWith(FILE_EXTENSION))) {
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
		document.setName(newName);
		setDocument(this.indexOf(document), document);
	}

	private OutputStream openOutputStream(int index) throws IOException {
		OutputStream outputStream = new FileOutputStream(getFile(index));
		if (compressedFiles) {
			outputStream = new DeflaterOutputStream(outputStream, true);
		}
		return outputStream;
	}

	private InputStream openInputStream(int index) throws IOException {
		InputStream inputStream = new FileInputStream(getFile(index));
		if (compressedFiles) {
			inputStream = new DeflaterInputStream(inputStream);
		}
		return inputStream;
	}

	private File getFile(int index) {
		return new File(directory, index + FILE_EXTENSION);
	}

	private static class GateObjectInputStream extends ObjectInputStream {

		public GateObjectInputStream(InputStream in) throws IOException {
			super(in);
		}

		@Override
		protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException, IOException {
			try {
				return Class.forName(desc.getName(), false, Gate.getClassLoader());
			} catch (Exception e) {
				return super.resolveClass(desc);
			}
		};
	}

}
