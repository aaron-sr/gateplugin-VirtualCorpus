package gate.virtualcorpus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import gate.Document;
import gate.DocumentExporter;
import gate.DocumentFormat;
import gate.Factory;
import gate.FeatureMap;
import gate.Gate;
import gate.Resource;
import gate.corpora.DocumentImpl;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.util.Files;
import gate.util.GateException;
import gate.util.GateRuntimeException;

@CreoleResource(name = "DirectoryCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents in a directory or directory tree")
public class DirectoryCorpus extends VirtualCorpus {
	private static final long serialVersionUID = -8485161260415382902L;
	private static final Logger logger = Logger.getLogger(DirectoryCorpus.class);

	protected URL directoryURL = null;
	protected List<String> extensions;
	protected Boolean recursive;
	protected Boolean hidden;
	protected String encoding;
	protected String mimeType;

	private File directory;

	@CreoleParameter(comment = "The directory URL where files will be read from", defaultValue = "")
	public void setDirectoryURL(URL dirURL) {
		this.directoryURL = dirURL;
	}

	public URL getDirectoryURL() {
		return this.directoryURL;
	}

	@Optional
	@CreoleParameter(comment = "A list of file extensions which will be loaded into the corpus. If not specified, all supported file extensions.", defaultValue = "")
	public void setExtensions(List<String> extensions) {
		this.extensions = extensions;
	}

	public List<String> getExtensions() {
		return extensions;
	}

	@Optional
	@CreoleParameter(comment = "Recursively get files from the directory", defaultValue = "false")
	public void setRecursive(Boolean value) {
		this.recursive = value;
	}

	public Boolean getRecursive() {
		return recursive;
	}

	@Optional
	@CreoleParameter(comment = "Get hidden files from the directory", defaultValue = "false")
	public void setHidden(Boolean hidden) {
		this.hidden = hidden;
	}

	public Boolean getHidden() {
		return hidden;
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
	@CreoleParameter(comment = "mimeType to read (and write, if exporterClassName is not set) document content", defaultValue = "")
	public final void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	public final String getMimeType() {
		return mimeType;
	}

	private List<File> files = new ArrayList<>();

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType(mimeType);
		checkValidExporterClassName(exporterClassName);
		if (!immutableCorpus) {
			throw new ResourceInstantiationException("mutable directory corpus currently not supported");
		}
		if (!hasValue(mimeType)) {
			for (String extension : extensions) {
				if (!DocumentFormat.getSupportedFileSuffixes().contains(extension)) {
					throw new ResourceInstantiationException(
							"cannot read file extension " + extension + ", no DocumentFormat available");
				}
				if (!readonlyDocuments) {
					if (getExporterForExtension(extension) == null) {
						throw new ResourceInstantiationException(
								"cannot write file extension " + extension + ", no DocumentExporter available");
					}
				}
			}
		}
		if (directoryURL == null) {
			throw new ResourceInstantiationException("directoryURL must be set");
		}
		try {
			directory = Files.fileFromURL(directoryURL).getCanonicalFile();
		} catch (Exception e) {
			throw new ResourceInstantiationException("directoryURL is not a valid file url", e);
		}

		if (!directory.isDirectory()) {
			throw new ResourceInstantiationException("Not a directory " + directory);
		}

		String[] supportedExtensions = !extensions.isEmpty() ? extensions.toArray(new String[0])
				: DocumentFormat.getSupportedFileSuffixes().toArray(new String[0]);

		Iterator<File> iterator = FileUtils.iterateFiles(directory, supportedExtensions, recursive);
		while (iterator.hasNext()) {
			File file = iterator.next();
			if (hidden || !file.isHidden()) {
				if (recursive) {
					try {
						file = file.getCanonicalFile();
					} catch (IOException e) {
						throw new ResourceInstantiationException("Could not get canonical path for " + file);
					}
				}
				files.add(file);
			}
		}
		initVirtualCorpus();
		return this;
	}

	protected static DocumentExporter getExporterForExtension(String fileExtension) {
		try {
			for (Resource resource : Gate.getCreoleRegister().getAllInstances("gate.DocumentExporter")) {
				DocumentExporter exporter = (DocumentExporter) resource;
				if (exporter.getDefaultExtension().contentEquals(fileExtension)) {
					return exporter;
				}
			}
		} catch (GateException e) {
		}
		return null;
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
		throw new GateRuntimeException("renaming document is not supported");
	}

	@Override
	protected int loadSize() throws Exception {
		return files.size();
	}

	@Override
	protected String loadDocumentName(int index) throws Exception {
		return directory.toURI().relativize(files.get(index).toURI()).getPath();
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		File file = files.get(index);
		String content = FileUtils.readFileToString(file);

		FeatureMap features = Factory.newFeatureMap();
		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		String documentName = getDocumentName(index);
		return (Document) Factory.createResource(DocumentImpl.class.getName(), params, features, documentName);
	}

	@Override
	protected Integer addDocuments(int index, Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void setDocument(int index, Document document) throws Exception {
		File file = files.get(index);

		try (OutputStream outputStream = new FileOutputStream(file)) {
			DocumentExporter exporter = null;
			if (hasValue(exporterClassName)) {
				exporter = getExporterForClassName(exporterClassName);
			}
			if (exporter == null && hasValue(mimeType)) {
				exporter = getExporterForMimeType(mimeType);
			}
			String extension = FilenameUtils.getExtension(file.getName());
			if (exporter == null && hasValue(extension)) {
				exporter = getExporterForExtension(extension);
			}
			if (exporter != null) {
				export(outputStream, document, exporter);
			} else if (hasValue(encoding)) {
				export(outputStream, document, encoding);
			} else {
				export(outputStream, document);
			}
		}
	}

	@Override
	protected Integer deleteDocuments(Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		throw new UnsupportedOperationException();
	}

}
