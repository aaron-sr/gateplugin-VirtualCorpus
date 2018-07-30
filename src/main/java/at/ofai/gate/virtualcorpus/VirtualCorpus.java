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

import java.io.FileFilter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.log4j.Logger;

import gate.Corpus;
import gate.Document;
import gate.creole.AbstractLanguageResource;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.Optional;
import gate.event.CorpusEvent;
import gate.event.CorpusListener;
import gate.util.GateRuntimeException;
import gate.util.MethodNotImplementedException;

/**
 * 
 * @author Johann Petrak
 */
public abstract class VirtualCorpus extends AbstractLanguageResource implements Corpus {
	private static final long serialVersionUID = -7769699900341757030L;
	private static Logger logger = Logger.getLogger(VirtualCorpus.class);

	/**
	 * Setter for the <code>readonly</code> LR initialization parameter.
	 *
	 * @param readonly
	 *            If set to true, documents will never be saved back. All methods
	 *            which would otherwise cause a document to get saved are silently
	 *            ignored.
	 */
	@Optional
	@CreoleParameter(comment = "If true, documents will never be saved", defaultValue = "false")
	public void setReadonly(Boolean readonly) {
		this.readonly = readonly;
	}

	public Boolean getReadonly() {
		return this.readonly;
	}

	protected Boolean readonly = true;

	@Override
	public void populate( // OK
			URL directory, FileFilter filter, String encoding, boolean recurseDirectories) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("populate(URL, FileFilter, boolean)"));
	}

	@Override
	public long populate(URL url, String docRoot, String encoding, int nrdocs, String docNamePrefix, String mimetype,
			boolean includeroot) {
		throw new gate.util.MethodNotImplementedException(
				notImplementedMessage("populate(URL, String, String, int, String, String, boolean"));
	}

	@Override
	public void populate( // OK
			URL directory, FileFilter filter, String encoding, String mimeType, boolean recurseDirectories) {
		throw new gate.util.MethodNotImplementedException(
				notImplementedMessage("populate(URL, FileFilter, String, String, boolean"));
	}

	public long populate(URL trecFile, String encoding, int numberOfDocumentsToExtract) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("populate(URL, String, int"));
	}

	protected String notImplementedMessage(String methodName) {
		return "Method " + methodName + " not supported for VirtualCorpus corpus " + this.getName() + " of class "
				+ this.getClass();
	}

	private List<String> documentNames = new ArrayList<String>();
	private List<Boolean> isLoadeds = new ArrayList<Boolean>();
	private Map<String, Document> loadedDocuments = new HashMap<String, Document>();

	protected void initDocuments(List<String> documentNames) {
		for (int i = 0; i < documentNames.size(); i++) {
			String docName = documentNames.get(i);
			this.documentNames.add(docName);
			this.isLoadeds.add(false);
		}
	}

	protected abstract Document readDocument(String docName);

	@Override
	public boolean isDocumentLoaded(int index) {
		if (index < 0 || index >= isLoadeds.size()) {
			throw new GateRuntimeException(
					"Document number " + index + " not in corpus " + this.getName() + " of size " + isLoadeds.size());
		}
		return isLoadeds.get(index);
	}

	public boolean isDocumentLoaded(Document doc) {
		String docName = doc.getName();
		if (!documentNames.contains(docName)) {
			throw new GateRuntimeException("Document " + docName + " is not contained in corpus " + this.getName());
		}
		return isDocumentLoaded(documentNames.indexOf(docName));
	}

	/**
	 * Unload a document from the corpus. This mimics what SerialCorpusImpl does:
	 * the document gets synced which in turn will save the document, then it gets
	 * removed from memory. Syncing will make our dummy datastore to invoke our own
	 * saveDocument method. The saveDocument method determines if the document
	 * should really be saved and how.
	 *
	 * @param doc
	 */
	@Override
	public void unloadDocument(Document doc) {
		String docName = doc.getName();
		logger.debug("DirectoryCorpus: called unloadDocument: " + docName);
		int index = documentNames.indexOf(docName);
		if (index == -1) {
			throw new RuntimeException("Document " + docName + " is not contained in corpus " + this.getName());
		}
		if (isDocumentLoaded(index)) {
			try {
				doc.sync();
			} catch (Exception ex) {
				throw new GateRuntimeException("Problem syncing document " + doc.getName(), ex);
			}
			loadedDocuments.remove(docName);
			isLoadeds.set(index, false);
		}
	}

	@Override
	public List<String> getDocumentNames() {
		List<String> newList = new ArrayList<String>(documentNames);
		return newList;
	}

	/**
	 * Return the name of the document with the given index from the corpus.
	 *
	 * @param i
	 *            the index of the document to return
	 * @return the name of the document with the given index
	 */
	@Override
	public String getDocumentName(int i) {
		return documentNames.get(i);
	}

	/**
	 * Return the document for the given index in the corpus. An
	 * IndexOutOfBoundsException is thrown when the index is not contained in the
	 * corpus. The document will be read from the file only if it is not already
	 * loaded. If it is already loaded a reference to that document is returned.
	 * 
	 * @param index
	 * @return
	 */
	@Override
	public final Document get(int index) {
		if (index < 0 || index >= documentNames.size()) {
			throw new IndexOutOfBoundsException(
					"Index " + index + " not in corpus " + this.getName() + " of size " + documentNames.size());
		}
		String docName = documentNames.get(index);
		if (isDocumentLoaded(index)) {
			Document doc = loadedDocuments.get(docName);
			return doc;
		}
		Document doc;
		try {
			doc = readDocument(docName);
		} catch (Exception ex) {
			throw new GateRuntimeException("Problem retrieving document data for " + docName, ex);
		}
		loadedDocuments.put(docName, doc);
		isLoadeds.set(index, true);
		return doc;
	}

	@Override
	public final int size() {
		return documentNames.size();
	}

	@Override
	public final int indexOf(Object docObj) {
		if (docObj instanceof Document) {
			Document doc = (Document) docObj;
			String docName = doc.getName();
			return documentNames.indexOf(docName);
		}
		return -1;
	}

	@Override
	public final boolean contains(Object docObj) {
		if (docObj instanceof Document) {
			Document doc = (Document) docObj;
			String docName = doc.getName();
			return documentNames.indexOf(docName) != -1;
		}
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object object : c) {
			if (!contains(object)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<Document> subList(int i1, int i2) {
		throw new MethodNotImplementedException(notImplementedMessage("subList(int,int)"));
	}

	@Override
	public Object[] toArray() {
		List<Document> documents = new ArrayList<>();
		for (int i = 0; i < documentNames.size(); i++) {
			Document document = get(i);
			documents.add(document);
		}
		return documents.toArray();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object[] toArray(Object[] x) {
		List<Document> documents = new ArrayList<>();
		for (int i = 0; i < documentNames.size(); i++) {
			Document document = get(i);
			documents.add(document);
		}
		return documents.toArray(x);
	}

	@Override
	public final Iterator<Document> iterator() {
		return new VirtualCorpusIterator(this);
	}

	private static class VirtualCorpusIterator implements Iterator<Document> {
		private VirtualCorpus corpus;
		private int nextIndex = 0;

		public VirtualCorpusIterator(VirtualCorpus corpus) {
			this.corpus = corpus;
		}

		@Override
		public boolean hasNext() {
			return (corpus.documentNames.size() > nextIndex);
		}

		@Override
		public Document next() {
			if (hasNext()) {
				return corpus.get(nextIndex++);
			} else {
				return null;
			}
		}

		@Override
		public void remove() {
			throw new MethodNotImplementedException();
		}
	}

	@Override
	public int lastIndexOf(Object docObj) {
		if (docObj instanceof Document) {
			Document doc = (Document) docObj;
			String docName = doc.getName();
			return documentNames.lastIndexOf(docName);
		}
		return -1;
	}

	@Override
	public boolean isEmpty() {
		return (documentNames.isEmpty());
	}

	@Override
	public final boolean add(Document e) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("add(Object)"));
	}

	@Override
	public final void add(int index, Document docObj) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("add(int,Object)"));
	}

	@Override
	public final Document set(int index, Document obj) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("set(int,Object)"));
	}

	@Override
	public final boolean addAll(Collection<? extends Document> c) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("addAll(Collection)"));
	}

	@Override
	public final boolean addAll(int i, Collection<? extends Document> c) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("addAll(int,Object)"));
	}

	@Override
	public final boolean retainAll(Collection<?> c) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("retainAll(Collection)"));
	}

	@Override
	public final boolean remove(Object o) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("remove(Object)"));
	}

	@Override
	public final boolean removeAll(Collection<?> c) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("removeAll(Collection)"));
	}

	@Override
	public final void clear() {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("clear()"));
	}

	@Override
	public final Document remove(int index) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("remove(int)"));
	}

	@Override
	public ListIterator<Document> listIterator(int i) {
		throw new MethodNotImplementedException(notImplementedMessage("listIterator(int)"));
	}

	@Override
	public ListIterator<Document> listIterator() {
		throw new MethodNotImplementedException(notImplementedMessage("listIterator()"));
	}

	protected List<CorpusListener> listeners = new ArrayList<CorpusListener>();

	@Override
	public void removeCorpusListener(CorpusListener listener) {
		listeners.remove(listener);
	}

	@Override
	public void addCorpusListener(CorpusListener listener) {
		listeners.add(listener);
	}

	protected void fireDocumentAdded(CorpusEvent e) {
		for (CorpusListener listener : listeners) {
			listener.documentAdded(e);
		}
	}

	protected void fireDocumentRemoved(CorpusEvent e) {
		for (CorpusListener listener : listeners) {
			listener.documentRemoved(e);
		}
	}

}
