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

import java.io.ByteArrayOutputStream;
import java.io.FileFilter;
import java.io.IOException;
import java.io.OutputStream;
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
import gate.DocumentExporter;
import gate.DocumentFormat;
import gate.Gate;
import gate.Resource;
import gate.creole.AbstractLanguageResource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.Optional;
import gate.event.CorpusEvent;
import gate.event.CorpusListener;
import gate.event.CreoleEvent;
import gate.event.CreoleListener;
import gate.event.DocumentEvent;
import gate.event.DocumentListener;
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
			throw new GateRuntimeException(e);
		}
	}

	protected Boolean readonly;
	protected Boolean immutable;
	protected String encoding;
	protected String mimeType;

	private DocumentListener documentListener = new VirtualCorpusDocumentListener(this);
	private boolean unloaded = false;

	@Optional
	@CreoleParameter(comment = "If true, document content changes will not be saved and document names cannot be renamed", defaultValue = "true")
	public final void setReadonly(Boolean readonly) {
		this.readonly = readonly;
	}

	public final Boolean getReadonly() {
		return this.readonly;
	}

	@Optional
	@CreoleParameter(comment = "If true, documents cannot be added or removed to the corpus", defaultValue = "true")
	public final void setImmutable(Boolean immutable) {
		this.immutable = immutable;
	}

	public final Boolean getImmutable() {
		return immutable;
	}

	@Optional
	@CreoleParameter(comment = "encoding to read and write document content", defaultValue = "utf-8")
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

	private List<String> documentNames = new ArrayList<String>();
	private Map<String, Document> documents = new HashMap<String, Document>();

	protected final void initVirtualCorpus(List<String> documentNames) {
		for (int i = 0; i < documentNames.size(); i++) {
			String documentName = documentNames.get(i);
			this.documentNames.add(documentName);
		}
		Gate.getCreoleRegister().addCreoleListener(new VirtualCorpusCreoleListener(this));
	}

	protected final static boolean hasValue(String string) {
		return string != null && string.trim().length() > 0;
	}

	protected final void checkValidMimeType() throws ResourceInstantiationException {
		if (hasValue(mimeType)) {
			if (!DocumentFormat.getSupportedMimeTypes().contains(mimeType)) {
				throw new ResourceInstantiationException(
						"cannot read mimeType" + mimeType + ", no DocumentFormat available");
			}
			if (!readonly) {
				if (getExporter(mimeType) == null) {
					throw new ResourceInstantiationException(
							"cannot write mimeType " + mimeType + ", no DocumentExporter available");
				}
			}
		}
	}

	protected DocumentExporter getExporter(String mimeType) {
		try {
			for (Resource resource : Gate.getCreoleRegister().getAllInstances("gate.DocumentExporter")) {
				DocumentExporter exporter = (DocumentExporter) resource;
				if (exporter.getMimeType().contentEquals(mimeType)) {
					return exporter;
				}
			}
			return null;
		} catch (Exception e) {
			throw new GateRuntimeException(e);
		}
	}

	protected void export(DocumentExporter exporter, Document document, OutputStream outputStream) {
		try {
			if (exporter == null) {
				outputStream.write(document.getContent().toString().getBytes(encoding));
			} else {
				exporter.export(document, outputStream);
			}
		} catch (IOException e) {
			throw new GateRuntimeException(e);
		}
	}

	protected String export(DocumentExporter exporter, Document document) {
		try {
			if (exporter == null) {
				return document.getContent().toString();
			} else {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				exporter.export(document, baos);
				return baos.toString();
			}
		} catch (IOException e) {
			throw new GateRuntimeException(e);
		}
	}

	private static class VirtualCorpusCreoleListener implements CreoleListener {

		private VirtualCorpus corpus;

		public VirtualCorpusCreoleListener(VirtualCorpus corpus) {
			this.corpus = corpus;
		}

		@Override
		public void resourceUnloaded(CreoleEvent e) {
			Resource resource = e.getResource();
			if (corpus.contains(resource)) {
				Document document = (Document) resource;
				corpus.unloadDocument(document);
			} else if (resource == corpus) {
				Gate.getCreoleRegister().removeCreoleListener(this);
				corpus.unloaded = true;
			}
		}

		@Override
		public void resourceRenamed(Resource resource, String oldName, String newName) {
			if (corpus.contains(resource)) {
				Document document = (Document) resource;
				if (corpus.readonly) {
					document.setName(oldName);
				} else {
					try {
						corpus.renameDocument(document, oldName, newName);
					} catch (Exception e) {
						throw new GateRuntimeException("Exception renaming the document", e);
					}
				}
			}
		}

		@Override
		public void resourceLoaded(CreoleEvent e) {

		}

		@Override
		public void datastoreOpened(CreoleEvent e) {

		}

		@Override
		public void datastoreCreated(CreoleEvent e) {

		}

		@Override
		public void datastoreClosed(CreoleEvent e) {

		}
	}

	private static class VirtualCorpusDocumentListener implements DocumentListener {

		private VirtualCorpus corpus;

		public VirtualCorpusDocumentListener(VirtualCorpus corpus) {
			this.corpus = corpus;
		}

		@Override
		public void annotationSetAdded(DocumentEvent e) {

		}

		@Override
		public void annotationSetRemoved(DocumentEvent e) {

		}

		@Override
		public void contentEdited(DocumentEvent e) {
			Document document = (Document) e.getSource();
			try {
				if (!corpus.readonly && !corpus.unloaded) {
					corpus.updateDocument(document);
				}
			} catch (Exception ex) {
				throw new GateRuntimeException("Exception updating the document", ex);
			}
		}

	}

	protected abstract void createDocument(Document document) throws Exception;

	protected abstract Document readDocument(String documentName) throws Exception;

	protected abstract void updateDocument(Document document) throws Exception;

	protected abstract void deleteDocument(Document document) throws Exception;

	protected abstract void renameDocument(Document document, String oldName, String newName) throws Exception;

	protected final void documentLoaded(String documentName, Document document) {
		if (documents.containsKey(documentName)) {
			throw new GateRuntimeException("document already loaded: " + documentName);
		}
		documents.put(documentName, document);
		document.addDocumentListener(documentListener);
	}

	protected void documentUnloaded(Document document) {
	}

	@Override
	public final boolean isDocumentLoaded(int index) {
		return documents.containsKey(documentNames.get(index));
	}

	@Override
	public final void unloadDocument(Document document) {
		String documentName = document.getName();
		int index = documentNames.indexOf(documentName);
		if (this.documentNames.contains(documentName) && isDocumentLoaded(index)) {
			try {
				if (!readonly && !unloaded) {
					updateDocument(document);
				}
			} catch (Exception e) {
				throw new GateRuntimeException("Problem updating document " + documentName, e);
			}
			documents.remove(documentName);
			document.removeDocumentListener(documentListener);
		}
	}

	@Override
	public final List<String> getDocumentNames() {
		return new ArrayList<String>(documentNames);
	}

	@Override
	public final String getDocumentName(int index) {
		return documentNames.get(index);
	}

	@Override
	public final Document get(int index) {
		String documentName = documentNames.get(index);
		if (isDocumentLoaded(index)) {
			return documents.get(documentName);
		}
		try {
			Document document = readDocument(documentName);
			documentLoaded(documentName, document);
			return document;
		} catch (Exception e) {
			throw new GateRuntimeException("Problem retrieving document data for " + documentName, e);
		}
	}

	@Override
	public final int size() {
		return documentNames.size();
	}

	@Override
	public final int indexOf(Object object) {
		if (object instanceof Document) {
			Document document = (Document) object;
			String documentName = document.getName();
			return documentNames.indexOf(documentName);
		}
		return -1;
	}

	@Override
	public final boolean contains(Object object) {
		if (object instanceof Document) {
			Document document = (Document) object;
			String documentName = document.getName();
			return documentNames.indexOf(documentName) != -1;
		}
		return false;
	}

	@Override
	public final boolean containsAll(Collection<?> c) {
		for (Object object : c) {
			if (!contains(object)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public final List<Document> subList(int i1, int i2) {
		List<Document> subList = new ArrayList<>();
		for (int i = i1; i < i2; i++) {
			subList.add(get(i));
		}
		return subList;
	}

	@Override
	public final Object[] toArray() {
		List<Document> documents = new ArrayList<>();
		for (int i = 0; i < documentNames.size(); i++) {
			Document document = get(i);
			documents.add(document);
		}
		return documents.toArray();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final Object[] toArray(Object[] x) {
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
	public final int lastIndexOf(Object object) {
		if (object instanceof Document) {
			Document document = (Document) object;
			String documentName = document.getName();
			return documentNames.lastIndexOf(documentName);
		}
		return -1;
	}

	@Override
	public final boolean isEmpty() {
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
		List<String> backupNames = new ArrayList<>(this.documentNames);
		Map<String, Document> backupDocuments = new HashMap<>(this.documents);

		try {
			documentNames.add(index, document.getName());
			documents.put(document.getName(), document);
			if (!unloaded) {
				createDocument(document);
			}
			fireDocumentAdded(index, document);
		} catch (Exception e) {
			this.documentNames = backupNames;
			this.documents = backupDocuments;
			throw new GateRuntimeException("Exception creating the document " + document.getName(), e);
		}
	}

	@Override
	public final Document set(int index, Document document) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		Document oldDocument = get(index);
		documentNames.set(index, document.getName());
		documents.put(document.getName(), document);
		fireDocumentRemoved(index, oldDocument);
		fireDocumentAdded(index, document);
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
	public final boolean addAll(int index, Collection<? extends Document> documents) {
		if (immutable) {
			throw new UnsupportedOperationException("this corpus is immutable");
		}
		List<String> newNames = new ArrayList<>();
		Map<String, Document> newDocuments = new HashMap<>();
		for (Document document : documents) {
			newNames.add(document.getName());
			newDocuments.put(document.getName(), document);
		}
		List<String> backupNames = new ArrayList<>(this.documentNames);
		Map<String, Document> backupDocuments = new HashMap<>(this.documents);

		boolean addAll = documentNames.addAll(index, newNames);
		this.documents.putAll(newDocuments);
		for (Document document : newDocuments.values()) {
			try {
				if (!unloaded) {
					createDocument(document);
				}
				fireDocumentAdded(this.indexOf(document), document);
			} catch (Exception e) {
				this.documentNames = backupNames;
				this.documents = backupDocuments;
				throw new GateRuntimeException("Exception creating the document " + document.getName(), e);
			}
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
		List<String> backupNames = new ArrayList<>(this.documentNames);
		Map<String, Document> backupDocuments = new HashMap<>(this.documents);

		boolean retainAll = this.documentNames.retainAll(documentNames);
		for (String documentName : this.documents.keySet()) {
			if (!documentNames.contains(documentName)) {
				Document document = this.documents.remove(documentName);
				try {
					if (!unloaded) {
						deleteDocument(document);
					}
					fireDocumentRemoved(indexes.remove(document), document);
				} catch (Exception e) {
					this.documentNames = backupNames;
					this.documents = backupDocuments;
					throw new GateRuntimeException("Exception deleting the document " + document.getName(), e);
				}
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
			List<String> backupNames = new ArrayList<>(this.documentNames);
			Map<String, Document> backupDocuments = new HashMap<>(this.documents);

			boolean remove = documentNames.remove(document.getName());
			documents.remove(document.getName());
			if (remove) {
				try {
					if (!unloaded) {
						deleteDocument(document);
					}
					fireDocumentRemoved(index, document);
				} catch (Exception e) {
					this.documentNames = backupNames;
					this.documents = backupDocuments;
					throw new GateRuntimeException("Exception deleting the document " + document.getName(), e);
				}
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
		List<String> backupNames = new ArrayList<>(this.documentNames);
		Map<String, Document> backupDocuments = new HashMap<>(this.documents);

		boolean removeAll = this.documentNames.removeAll(documentNames);
		for (Document document : documents) {
			this.documents.remove(document.getName());
			try {
				if (!unloaded) {
					deleteDocument(document);
				}
				fireDocumentRemoved(indexes.remove(document), document);
			} catch (Exception e) {
				this.documentNames = backupNames;
				this.documents = backupDocuments;
				throw new GateRuntimeException("Exception deleting the document " + document.getName(), e);
			}
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
		List<String> backupNames = new ArrayList<>(this.documentNames);
		Map<String, Document> backupDocuments = new HashMap<>(this.documents);

		this.documentNames.clear();
		this.documents.clear();
		for (Document document : indexes.keySet()) {
			try {
				if (!unloaded) {
					deleteDocument(document);
				}
				fireDocumentRemoved(indexes.get(document), document);
			} catch (Exception e) {
				this.documentNames = backupNames;
				this.documents = backupDocuments;
				throw new GateRuntimeException("Exception deleting the document" + document.getName(), e);
			}
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
	public final ListIterator<Document> listIterator(int i) {
		return new VirtualCorpusListIterator(this, i);
	}

	@Override
	public final ListIterator<Document> listIterator() {
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
	public long populate(URL url, String docRoot, String encoding, int nrdocs, String docNadocumentNamex,
			String mimetype, boolean includeroot) {
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

	protected final List<String> splitUserInput(String string) {
		List<String> values = new ArrayList<>();
		for (String value : string.split(",")) {
			values.add(value.trim());
		}
		return values;
	}

}
