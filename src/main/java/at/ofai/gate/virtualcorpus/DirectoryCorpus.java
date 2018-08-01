/*
 * Copyright (c) 2010- Austrian Research Institute for Artificial Intelligence (OFAI). 
 * Copyright (C) 2014-2016 The University of Sheffield.
 *
 * This file is part of gateplugin-VirtualCorpus
 * (see https://github.com/johann-petrak/gateplugin-VirtualCorpus)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software. If not, see <http://www.gnu.org/licenses/>.
 */

package at.ofai.gate.virtualcorpus;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
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
import gate.util.GateRuntimeException;

/**
 * A Corpus LR that mirrors files in a directory.
 * <p>
 * NOTE: If you use the "Save as XML" option from the LR's context menu, be
 * careful not specify the directory where the corpus saves documents as the
 * target directory for the "Save as XML" function -- this might produce
 * unexpected results. Even if a different directory is specified, the "Save as
 * XML" function will still also re-save the documents in the corpus directory
 * unless the <code>saveDocuments</code> option is set to false.
 * 
 */
@CreoleResource(name = "DirectoryCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents in a directory or directory tree")
public class DirectoryCorpus extends VirtualCorpus {
	private static final long serialVersionUID = -8485161260415382902L;
	private static final Logger logger = Logger.getLogger(DirectoryCorpus.class);

	protected URL directoryURL = null;
	protected List<String> extensions;
	protected Boolean recursive;
	protected Boolean hidden;

	private File directory;

	@CreoleParameter(comment = "The directory URL where files will be read from")
	public void setDirectoryURL(URL dirURL) {
		this.directoryURL = dirURL;
	}

	public URL getDirectoryURL() {
		return this.directoryURL;
	}

	@Optional
	@CreoleParameter(comment = "A list of file extensions which will be loaded into the corpus. If not specified, all supported file extensions.")
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

	@Override
	public Resource init() throws ResourceInstantiationException {
		logger.info("DirectoryCorpus: calling init");

		for (String extension : extensions) {
			if (!DocumentFormat.getSupportedFileSuffixes().contains(extension)) {
				throw new ResourceInstantiationException(
						"cannot read file extension " + extension + ", no DocumentFormat available");
			}
			if (!readonly && getExporterForExtension(extension) == null) {
				throw new ResourceInstantiationException(
						"cannot write file extension " + extension + ", no DocumentExporter available");
			}
		}

		if (directoryURL == null) {
			throw new ResourceInstantiationException("directoryURL must be set");
		}

		try {
			directory = Files.fileFromURL(directoryURL).getCanonicalFile();
		} catch (Exception e) {
			throw new ResourceInstantiationException("directoryURL is not a valid file url");
		}

		if (!directory.isDirectory()) {
			throw new ResourceInstantiationException("Not a directory " + directory);
		}

		String[] supportedExtensions = !extensions.isEmpty() ? extensions.toArray(new String[0])
				: DocumentFormat.getSupportedFileSuffixes().toArray(new String[0]);

		List<String> documentNames = new ArrayList<>();
		Iterator<File> iterator = FileUtils.iterateFiles(directory, supportedExtensions, recursive);
		while (iterator.hasNext()) {
			File file = iterator.next();
			String filename = file.getName();
			if (hidden || !file.isHidden()) {
				if (recursive) {
					try {
						file = file.getCanonicalFile();
					} catch (IOException e) {
						throw new ResourceInstantiationException("Could not get canonical path for " + file);
					}
					filename = directory.toURI().relativize(file.toURI()).getPath();
				}
				documentNames.add(filename);
			}
		}
		initDocuments(documentNames);
		return this;
	}

	protected DocumentExporter getExporterForExtension(String fileExtension) {
		try {
			for (Resource resource : Gate.getCreoleRegister().getAllInstances("gate.DocumentExporter")) {
				DocumentExporter exporter = (DocumentExporter) resource;
				if (exporter.getDefaultExtension().contentEquals(fileExtension)) {
					return exporter;
				}
			}
			return null;
		} catch (Exception e) {
			throw new GateRuntimeException(e);
		}
	}

	@Override
	protected Document readDocument(String documentName) {
		File documentFile = new File(directory, documentName);
		URL documentURL;
		try {
			documentURL = documentFile.toURI().toURL();
		} catch (MalformedURLException e) {
			throw new GateRuntimeException("Could not create URL for document name " + documentName, e);
		}
		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_URL_PARAMETER_NAME, documentURL);
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		try {
			return (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, documentName);
		} catch (ResourceInstantiationException e) {
			throw new GateRuntimeException("Could not create Document from file " + documentFile, e);
		}
	}

	@Override
	protected void createDocument(Document document) {
		String documentName = document.getName();
		File documentFile = new File(directory, documentName);
		try {
			documentFile.createNewFile();
		} catch (IOException e) {
			throw new GateRuntimeException(e);
		}
	}

	@Override
	protected void updateDocument(Document document) {
		String documentName = document.getName();
		DocumentExporter exporter = mimeType != null && mimeType.length() > 0 ? getExporter(mimeType)
				: getExporterForExtension(FilenameUtils.getExtension(documentName));

		File documentFile = new File(directory, documentName);
		try {
			exporter.export(document, documentFile);
		} catch (IOException e) {
			throw new GateRuntimeException("Could not save file: " + documentFile, e);
		}

	}

	@Override
	protected void deleteDocument(Document document) {
		String documentName = document.getName();
		File documentFile = new File(directory, documentName);
		documentFile.delete();
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) {
		File oldFile = new File(directory, oldName);
		File newFile = new File(directory, newName);
		oldFile.renameTo(newFile);
	}

}
