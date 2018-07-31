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
import gate.persist.PersistenceException;
import gate.util.GateRuntimeException;
import gate.util.persistence.PersistenceManager;

public abstract class VirtualCorpus extends AbstractLanguageResource implements Corpus {
	private static final long serialVersionUID = -7769699900341757030L;
	private static Logger logger = Logger.getLogger(VirtualCorpus.class);

	static {
		try {
			PersistenceManager.registerPersistentEquivalent(VirtualCorpus.class, VirtualCorpusPersistence.class);
		} catch (PersistenceException e) {
			e.printStackTrace();
		}
	}

	protected Boolean readonly;
	protected Boolean immutable;

	@Optional
	@CreoleParameter(comment = "If true, documents will never be saved", defaultValue = "true")
	public void setReadonly(Boolean readonly) {
		this.readonly = readonly;
	}

	public Boolean getReadonly() {
		return this.readonly;
	}

	@Optional
	@CreoleParameter(comment = "If true, documents cannot be added or removed to corpus", defaultValue = "true")
	public void setImmutable(Boolean immutable) {
		this.immutable = immutable;
	}

	public Boolean getImmutable() {
		return immutable;
	}

	private List<String> documentNames = new ArrayList<String>();
	private Map<String, Document> documents = new HashMap<String, Document>();

	protected void initDocuments(List<String> documentNames) {
		for (int i = 0; i < documentNames.size(); i++) {
			String docName = documentNames.get(i);
			this.documentNames.add(docName);
		}
	}

	protected abstract void createDocument(Document document);

	protected abstract Document readDocument(String documentName);

	protected abstract void updateDocument(Document document);

	protected abstract void deleteDocument(Document document);

	protected abstract void renameDocument(Document document, String oldName, String newName);

	@Override
	public boolean isDocumentLoaded(int index) {
		if (index < 0 || index >= documentNames.size()) {
			throw new GateRuntimeException("Document number " + index + " not in corpus " + this.getName() + " of size "
					+ documentNames.size());
		}
		return documents.containsKey(documentNames.get(index));
	}

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
			documents.remove(docName);
		}
	}

	@Override
	public List<String> getDocumentNames() {
		List<String> newList = new ArrayList<String>(documentNames);
		return newList;
	}

	@Override
	public String getDocumentName(int i) {
		return documentNames.get(i);
	}

	@Override
	public final Document get(int index) {
		if (index < 0 || index >= documentNames.size()) {
			throw new IndexOutOfBoundsException(
					"Index " + index + " not in corpus " + this.getName() + " of size " + documentNames.size());
		}
		String docName = documentNames.get(index);
		if (isDocumentLoaded(index)) {
			Document doc = documents.get(docName);
			return doc;
		}
		Document doc;
		try {
			doc = readDocument(docName);
		} catch (Exception ex) {
			throw new GateRuntimeException("Problem retrieving document data for " + docName, ex);
		}
		documents.put(docName, doc);
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
		List<Document> subList = new ArrayList<>();
		for (int i = i1; i < i2; i++) {
			subList.add(get(i));
		}
		return subList;
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
			if (corpus.immutable) {
				throw new UnsupportedOperationException("this corpus is immutable");
			}
			corpus.remove(nextIndex);
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
	public final boolean add(Document document) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		add(size(), document);
		return true;
	}

	@Override
	public final void add(int index, Document document) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		documentNames.add(index, document.getName());
		documents.put(document.getName(), document);
		createDocument(document);
		fireDocumentAdded(index, document);
	}

	@Override
	public final Document set(int index, Document document) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		Document oldDocument = get(index);
		documentNames.set(index, document.getName());
		documents.put(document.getName(), document);
		updateDocument(document);
		return oldDocument;
	}

	@Override
	public final boolean addAll(Collection<? extends Document> documents) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		return addAll(size(), documents);
	}

	@Override
	public final boolean addAll(int i, Collection<? extends Document> documents) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		List<String> newDocumentNames = new ArrayList<>();
		Map<String, Document> newDocuments = new HashMap<>();
		for (Document document : documents) {
			newDocumentNames.add(document.getName());
			newDocuments.put(document.getName(), document);
		}
		boolean addAll = documentNames.addAll(newDocumentNames);
		this.documents.putAll(newDocuments);
		for (Document document : newDocuments.values()) {
			createDocument(document);
			fireDocumentAdded(this.indexOf(document), document);
		}
		return addAll;
	}

	@Override
	public final boolean retainAll(Collection<?> collection) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		List<String> documentNames = new ArrayList<>();
		Map<Document, Integer> indexes = new HashMap<>();
		for (Object object : this) {
			if (object instanceof Document && this.contains(object)) {
				Document document = (Document) object;
				documentNames.add(document.getName());
				indexes.put(document, this.indexOf(document));
			}
		}
		boolean retainAll = this.documentNames.retainAll(documentNames);
		for (String documentName : this.documents.keySet()) {
			if (!documentNames.contains(documentName)) {
				Document document = this.documents.remove(documentName);
				this.deleteDocument(document);
				fireDocumentRemoved(indexes.remove(document), document);
			}
		}
		return retainAll;
	}

	@Override
	public final boolean remove(Object object) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		if (object instanceof Document) {
			Document document = (Document) object;
			int index = this.indexOf(document);
			boolean remove = documentNames.remove(document.getName());
			documents.remove(document.getName());
			if (remove) {
				deleteDocument(document);
				fireDocumentRemoved(index, document);
			}
			return remove;
		}
		return false;
	}

	@Override
	public final boolean removeAll(Collection<?> collection) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		List<Document> documents = new ArrayList<>();
		List<String> documentNames = new ArrayList<>();
		Map<Document, Integer> indexes = new HashMap<>();
		for (Object object : collection) {
			if (object instanceof Document && this.contains(object)) {
				Document document = (Document) object;
				documents.add(document);
				documentNames.add(document.getName());
				indexes.put(document, this.indexOf(document));
			}
		}
		boolean removeAll = this.documentNames.removeAll(documentNames);
		for (Document document : documents) {
			this.documents.remove(document.getName());
			deleteDocument(document);
			fireDocumentRemoved(indexes.remove(document), document);
		}
		return removeAll;
	}

	@Override
	public final void clear() {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		Map<Document, Integer> indexes = new HashMap<>();
		for (Document document : this.documents.values()) {
			indexes.put(document, this.indexOf(document));
		}
		this.documentNames.clear();
		this.documents.clear();
		for (Document document : indexes.keySet()) {
			deleteDocument(document);
			fireDocumentRemoved(indexes.get(document), document);
		}
	}

	@Override
	public final Document remove(int index) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		Document document = get(index);
		remove(document);
		return document;
	}

	@Override
	public ListIterator<Document> listIterator(int i) {
		return new VirtualCorpusListIterator(this, i);
	}

	@Override
	public ListIterator<Document> listIterator() {
		return listIterator(0);
	}

	private static class VirtualCorpusListIterator implements ListIterator<Document> {

		private VirtualCorpus corpus;
		private int nextIndex;

		public VirtualCorpusListIterator(VirtualCorpus corpus, int nextIndex) {
			this.corpus = corpus;
			this.nextIndex = nextIndex;
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
		public boolean hasPrevious() {
			return nextIndex > 0 && !corpus.documentNames.isEmpty();
		}

		@Override
		public Document previous() {
			if (hasPrevious()) {
				return corpus.get(--nextIndex);
			} else {
				return null;
			}
		}

		@Override
		public int nextIndex() {
			return nextIndex + 1;
		}

		@Override
		public int previousIndex() {
			return nextIndex - 1;
		}

		@Override
		public void remove() {
			if (corpus.immutable) {
				throw new UnsupportedOperationException("this corpus is immutable");
			}
			corpus.remove(nextIndex);
		}

		@Override
		public void set(Document document) {
			if (corpus.immutable) {
				throw new UnsupportedOperationException("this corpus is immutable");
			}
			corpus.set(nextIndex, document);
		}

		@Override
		public void add(Document document) {
			if (corpus.immutable) {
				throw new UnsupportedOperationException("this corpus is immutable");
			}
			corpus.add(nextIndex, document);
		}

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

	protected void fireDocumentAdded(int index, Document document) {
		CorpusEvent event = new CorpusEvent(this, document, index, CorpusEvent.DOCUMENT_ADDED);
		for (CorpusListener listener : listeners) {
			listener.documentAdded(event);
		}
	}

	protected void fireDocumentRemoved(int index, Document document) {
		CorpusEvent event = new CorpusEvent(this, document, index, CorpusEvent.DOCUMENT_REMOVED);
		for (CorpusListener listener : listeners) {
			listener.documentAdded(event);
		}
	}

	@Override
	public void populate(URL directory, FileFilter filter, String encoding, boolean recurseDirectories) {
		throw new gate.util.MethodNotImplementedException(notImplementedMessage("populate(URL, FileFilter, boolean)"));
	}

	@Override
	public long populate(URL url, String docRoot, String encoding, int nrdocs, String docNamePrefix, String mimetype,
			boolean includeroot) {
		throw new gate.util.MethodNotImplementedException(
				notImplementedMessage("populate(URL, String, String, int, String, String, boolean"));
	}

	@Override
	public void populate(URL directory, FileFilter filter, String encoding, String mimeType,
			boolean recurseDirectories) {
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

}
