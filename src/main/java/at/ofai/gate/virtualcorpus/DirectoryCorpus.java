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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
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

/**
 * A Corpus LR that mirrors files in a directory. In the default configuration,
 * just the <code>directoryURL</code> parameter is specified at creation and all
 * files that have a file extension of ".xml" and are not hidden are accessible
 * as documents through that corpus and automatically written back to the
 * directory when sync'ed or when unloaded (which does an implicit sync). If the
 * parameter <code>outDirectoryURL</code> is also specified, the corpus reflects
 * all the files from the <code>directoryURL</code> directory but writes any
 * changed documents into the directory <code>outDirectoryURL</code>. If the
 * parameter <code>saveDocuments</code> is set to false, nothing is ever written
 * to either of the directories.
 * <p>
 * The main purpose of this Corpus implementation is that through it a serial
 * controller can directly read and write from files stored in a directory. This
 * makes it much easier to share working pipelines between pipeline developers,
 * especially when the pipeline files are checked into SCS.
 * <p>
 * Documents will always get saved to either the original file or to a file in
 * the outDocumentURL directory whenever the document is synced or unloaded.
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

	protected File backingDirectoryFile;

	@CreoleParameter(comment = "The directory URL where files will be read from")
	public void setDirectoryURL(URL dirURL) {
		this.directoryURL = dirURL;
	}

	public URL getDirectoryURL() {
		return this.directoryURL;
	}

	protected URL directoryURL = null;

	@Optional
	@CreoleParameter(comment = "A list of file extensions which will be loaded into the corpus. If not specified, all supported file extensions. ")
	public void setExtensions(List<String> extensions) {
		this.extensions = extensions;
	}

	public List<String> getExtensions() {
		return extensions;
	}

	protected List<String> extensions;

	@Optional
	@CreoleParameter(comment = "Recursively get files from the directory", defaultValue = "false")
	public void setRecurseDirectory(Boolean value) {
		this.recurseDirectory = value;
	}

	public Boolean getRecurseDirectory() {
		return recurseDirectory;
	}

	protected Boolean recurseDirectory;

	Map<String, DocumentExporter> extension2Exporter = new HashMap<String, DocumentExporter>();

	/**
	 * Initializes the DirectoryCorpus LR
	 * 
	 * @return
	 * @throws ResourceInstantiationException
	 */
	@Override
	public Resource init() throws ResourceInstantiationException {
		logger.info("DirectoryCorpus: calling init");
		if (directoryURL == null) {
			throw new ResourceInstantiationException("directoryURL must be set");
		}
		// first of all, create a map that contains all the supported extensions
		// as keys and the corresponding documente exporter as value.

		// First, get all the supported extensions for reading files
		Set<String> readExtensions = DocumentFormat.getSupportedFileSuffixes();
		logger.info("DirectoryCorpus/init readExtensions=" + readExtensions);
		Set<String> supportedExtensions = new HashSet<String>();

		// if we also want to write, we have to limit the supported extensions
		// to those where we have an exporter and also we need to remember which
		// exporter supports which extensions
		if (!getReadonly()) {
			List<Resource> des = null;
			try {
				// Now get all the Document exporters
				des = Gate.getCreoleRegister().getAllInstances("gate.DocumentExporter");
			} catch (GateException ex) {
				throw new ResourceInstantiationException("Could not get the document exporters", ex);
			}
			for (Resource r : des) {
				DocumentExporter d = (DocumentExporter) r;
				if (readExtensions.contains(d.getDefaultExtension())) {
					extension2Exporter.put(d.getDefaultExtension(), d);
					supportedExtensions.add(d.getDefaultExtension());
				}
			}
		} else {
			supportedExtensions.addAll(readExtensions);
		}
		logger.info("DirectoryCorpus/init supportedExtensions=" + readExtensions);

		// now check if an extension list was specified by the user. If no, nothing
		// needs to be done. If yes, remove all the extensions from the
		// extnesion2Exporter
		// map which were not specified and warn about all the extensions specified
		// for which we do not have an entry. Also remove them from the
		// supportedExtensions set
		if (getExtensions() != null && !getExtensions().isEmpty()) {
			logger.info("DirectoryCorpu/init getExtgension is not empty: " + getExtensions());
			for (String ext : getExtensions()) {
				if (!supportedExtensions.contains(ext)) {
					logger.warn("DirectoryCorpus warning: extension is not supported: " + ext);
				}
			}
			// now remove all the extensions which are not specified
			Iterator<String> it = supportedExtensions.iterator();
			while (it.hasNext()) {
				String ext = it.next();
				logger.info("DirectoryCorpus/init checking supported extension: " + ext);
				if (!getExtensions().contains(ext)) {
					logger.info("DirectoryCorpus/init removing extension: " + ext);
					it.remove();
					extension2Exporter.remove(ext);
				}
			}
		}
		logger.info("DirectoryCorpus/init supportedExtensions after parms: " + supportedExtensions);
		logger.info("DirectoryCorpus/init exporter map: " + extension2Exporter);

		if (supportedExtensions.isEmpty()) {
			throw new ResourceInstantiationException(
					"DirectoryCorpus could not be created, no file format supported or loaded");
		}

		backingDirectoryFile = Files.fileFromURL(directoryURL);
		try {
			backingDirectoryFile = backingDirectoryFile.getCanonicalFile();
		} catch (IOException ex) {
			throw new ResourceInstantiationException("Cannot get canonical file for " + backingDirectoryFile, ex);
		}
		if (!backingDirectoryFile.isDirectory()) {
			throw new ResourceInstantiationException("Not a directory " + backingDirectoryFile);
		}

		Iterator<File> fileIt = FileUtils.iterateFiles(backingDirectoryFile, supportedExtensions.toArray(new String[0]),
				getRecurseDirectory());
		List<String> documentNames = new ArrayList<>();
		while (fileIt.hasNext()) {
			File file = fileIt.next();
			// if recursion was specified, we need to get the relative file path
			// relative to the root directory. This is done by getting the canonical
			// full path name for both the directory and the file and then
			// relativizing the path.
			String filename = file.getName();
			if (!file.isHidden()) {
				if (getRecurseDirectory()) {
					try {
						file = file.getCanonicalFile();
					} catch (IOException ex) {
						throw new ResourceInstantiationException("Could not get canonical path for " + file);
					}
					filename = backingDirectoryFile.toURI().relativize(file.toURI()).getPath();
				}
				documentNames.add(filename);
			}
		}
		initDocuments(documentNames);
		return this;
	}

	protected void saveDocument(Document doc) {
		if (getReadonly()) {
			return;
		}
		String docName = doc.getName();
		// get the extension and then look up the document exporter for that
		// extension which will be used to do the actual saving.
		int extDotPos = docName.lastIndexOf(".");
		if (extDotPos <= 0) {
			throw new GateRuntimeException(
					"Did not find a file name extensions when trying to save document " + docName);
		}
		String ext = docName.substring(extDotPos + 1);
		if (ext.isEmpty()) {
			throw new GateRuntimeException("Encountered empty extension when trying to save document " + docName);
		}
		DocumentExporter de = extension2Exporter.get(ext);
		logger.info("DirectoryCorpus/saveDocument exit is " + ext + " exporter " + de);
		File docFile = new File(backingDirectoryFile, docName);
		try {
			logger.info(
					"DirectoryCorpus/saveDocument trying to save document " + doc.getName() + " using exporter " + de);
			de.export(doc, docFile);
			logger.info("DirectoryCorpus/saveDocument saved: " + doc.getName());
		} catch (IOException ex) {
			throw new GateRuntimeException("Could not save file: " + docFile, ex);
		}
	}

	@Override
	protected Document readDocument(String docName) {
		File docFile = new File(backingDirectoryFile, docName);
		URL docURL;
		try {
			docURL = docFile.toURI().toURL();
		} catch (MalformedURLException ex) {
			throw new GateRuntimeException("Could not create URL for document name " + docName, ex);
		}
		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_URL_PARAMETER_NAME, docURL);
		try {
			return (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, docName);
		} catch (ResourceInstantiationException ex) {
			throw new GateRuntimeException("Could not create Document from file " + docFile, ex);
		}
	}

}
