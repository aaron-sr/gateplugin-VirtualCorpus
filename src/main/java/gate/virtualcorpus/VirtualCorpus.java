package gate.virtualcorpus;

import java.io.ByteArrayOutputStream;
import java.io.FileFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Corpus;
import gate.Document;
import gate.DocumentExporter;
import gate.DocumentFormat;
import gate.FeatureMap;
import gate.Gate;
import gate.Resource;
import gate.creole.AbstractLanguageResource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.Optional;
import gate.event.AnnotationEvent;
import gate.event.AnnotationListener;
import gate.event.AnnotationSetEvent;
import gate.event.AnnotationSetListener;
import gate.event.CorpusEvent;
import gate.event.CorpusListener;
import gate.event.CreoleEvent;
import gate.event.CreoleListener;
import gate.event.DocumentEvent;
import gate.event.DocumentListener;
import gate.event.FeatureMapListener;
import gate.event.RelationSetEvent;
import gate.event.RelationSetListener;
import gate.persist.PersistenceException;
import gate.relations.Relation;
import gate.relations.RelationSet;
import gate.util.GateException;
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

	protected Boolean readonlyDocuments = true;
	protected Boolean immutableCorpus = true;
	protected Integer cacheDocumentNames;

	@Optional
	@CreoleParameter(comment = "cache n last document names", defaultValue = "100000")
	public void setCacheDocumentNames(Integer cacheDocumentNames) {
		this.cacheDocumentNames = cacheDocumentNames;
	}

	public Integer getCacheDocumentNames() {
		return cacheDocumentNames;
	}

	private VirtualCorpusCreoleListener creoleListener;
	private boolean unloaded = false;

	private Integer size;
	private transient int modCount = 0;
	private SortedMap<Integer, Document> loadedDocuments = new TreeMap<>();
	private SortedMap<Integer, String> loadedDocumentNames = new TreeMap<>();
	private Set<Integer> lruDocumentNameIndexes = new LinkedHashSet<>();
	private Map<Document, DocumentChangeObserver> documentChangeObservers = new HashMap<>();

	protected final void initVirtualCorpus() {
		creoleListener = new VirtualCorpusCreoleListener(this);
		Gate.getCreoleRegister().addCreoleListener(creoleListener);
	}

	protected final void cleanupVirtualCorpus() {
		Gate.getCreoleRegister().removeCreoleListener(creoleListener);
	}

	protected final static boolean hasValue(String string) {
		return string != null && string.trim().length() > 0;
	}

	protected final List<String> splitUserInput(String string) {
		List<String> values = new ArrayList<>();
		for (String value : string.split(",")) {
			String trim = value.trim();
			if (trim.length() > 0) {
				values.add(trim);
			}
		}
		return values;
	}

	protected final static <E> List<E> toList(E e) {
		List<E> list = new ArrayList<>();
		list.add(e);
		return list;
	}

	protected final static void checkValidMimeType(String mimeType, boolean required)
			throws ResourceInstantiationException {
		if (required && !hasValue(mimeType)) {
			throw new ResourceInstantiationException("no mimeType provided");
		}
		if (hasValue(mimeType)) {
			if (!DocumentFormat.getSupportedMimeTypes().contains(mimeType)) {
				throw new ResourceInstantiationException(
						"cannot read mimeType" + mimeType + ", no DocumentFormat available");
			}
		}
	}

	protected final static void checkValidExporterClassName(String exporterClassName, boolean required)
			throws ResourceInstantiationException {
		if (required && !hasValue(exporterClassName)) {
			throw new ResourceInstantiationException("no exporterClassName provided");
		}
		if (hasValue(exporterClassName)) {
			Class<?> exporterClass;
			try {
				exporterClass = Gate.getClassLoader().loadClass(exporterClassName);
			} catch (ClassNotFoundException e) {
				throw new ResourceInstantiationException(e);
			}
			if (!DocumentExporter.class.isAssignableFrom(exporterClass)) {
				throw new ResourceInstantiationException("exporterClassName must be subclass of gate.DocumentExporter");
			}
			if (DocumentExporter.getInstance(exporterClassName) == null) {
				throw new ResourceInstantiationException("no exporter instance found for class " + exporterClassName);
			}
		}
	}

	protected final static DocumentExporter getExporterForClassName(String exporterClassName) {
		if (hasValue(exporterClassName)) {
			return DocumentExporter.getInstance(exporterClassName);
		}
		return null;
	}

	protected final static DocumentExporter getExporterForMimeType(String mimeType) {
		List<Resource> exporters;
		try {
			exporters = Gate.getCreoleRegister().getAllInstances("gate.DocumentExporter");
		} catch (GateException e) {
			throw new GateRuntimeException(e);
		}
		for (Resource resource : exporters) {
			DocumentExporter exporter = (DocumentExporter) resource;
			if (exporter.getMimeType().contentEquals(mimeType)) {
				return exporter;
			}
		}
		return null;
	}

	protected final static void export(OutputStream outputStream, Document document, DocumentExporter exporter) {
		try {
			exporter.export(document, outputStream);
		} catch (IOException e) {
			throw new GateRuntimeException(e);
		}
	}

	protected final static void export(OutputStream outputStream, Document document) {
		try {
			outputStream.write(document.getContent().toString().getBytes());
		} catch (IOException e) {
			throw new GateRuntimeException(e);
		}
	}

	protected final static void export(OutputStream outputStream, Document document, String encoding) {
		try {
			outputStream.write(document.getContent().toString().getBytes(encoding));
		} catch (IOException e) {
			throw new GateRuntimeException(e);
		}
	}

	protected byte[] export(DocumentExporter exporter, Document document) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			export(baos, document, exporter);
			return baos.toByteArray();
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
			if (resource instanceof Document) {
				Document document = (Document) resource;
				corpus.unloadDocument(document);
			} else if (resource == corpus) {
				Gate.getCreoleRegister().removeCreoleListener(this);
				corpus.unload();
			}
		}

		@Override
		public void resourceRenamed(Resource resource, String oldName, String newName) {
			if (corpus.contains(resource)) {
				Document document = (Document) resource;
				if (corpus.readonlyDocuments) {
					document.setName(oldName);
				} else {
					try {
						corpus.renameDocument(document, oldName, newName);
					} catch (Exception e) {
						throw new GateRuntimeException("cannot rename document " + document, e);
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

	private static class DocumentChangeObserver implements DocumentListener, AnnotationSetListener, AnnotationListener,
			RelationSetListener, FeatureMapListener {

		private Document document;
		private boolean changed = false;

		private Map<String, AnnotationSet> annotationSets = new HashMap<>();

		public DocumentChangeObserver(Document document) {
			this.document = document;

			registerDocument();
		}

		private void registerDocument() {
			document.addDocumentListener(this);
			registerFeatureMap(document.getFeatures());

			registerAnnotationSet(document.getAnnotations());
			for (AnnotationSet annotationSet : document.getNamedAnnotationSets().values()) {
				registerAnnotationSet(annotationSet);
			}
		}

		private void unregisterDocument() {
			document.removeDocumentListener(this);
			unregisterFeatureMap(document.getFeatures());

			unregisterAnnotationSet(document.getAnnotations());
			for (AnnotationSet annotationSet : document.getNamedAnnotationSets().values()) {
				unregisterAnnotationSet(annotationSet);
			}
		}

		private void registerAnnotationSet(AnnotationSet annotationSet) {
			annotationSet.addAnnotationSetListener(this);
			if (annotationSet.getName() != null && annotationSet.getName().length() > 0) {
				annotationSets.put(annotationSet.getName(), annotationSet);
			}
			for (Annotation annotation : annotationSet) {
				registerAnnotation(annotation);
			}
			registerRelationSet(annotationSet.getRelations());
		}

		private void unregisterAnnotationSet(AnnotationSet annotationSet) {
			annotationSet.removeAnnotationSetListener(this);
			if (annotationSet.getName() != null && annotationSet.getName().length() > 0) {
				annotationSets.remove(annotationSet.getName());
			}
			for (Annotation annotation : annotationSet) {
				unregisterAnnotation(annotation);
			}
			unregisterRelationSet(annotationSet.getRelations());
		}

		private void registerAnnotation(Annotation annotation) {
			annotation.addAnnotationListener(this);
			registerFeatureMap(annotation.getFeatures());
		}

		private void unregisterAnnotation(Annotation annotation) {
			annotation.removeAnnotationListener(this);
			unregisterFeatureMap(annotation.getFeatures());
		}

		private void registerRelationSet(RelationSet relationSet) {
			relationSet.addRelationSetListener(this);
			for (Relation relation : relationSet) {
				registerRelation(relation);
			}
		}

		private void unregisterRelationSet(RelationSet relationSet) {
			relationSet.removeRelationSetListener(this);
			for (Relation relation : relationSet) {
				unregisterRelation(relation);
			}
		}

		private void registerRelation(Relation relation) {
			registerFeatureMap(relation.getFeatures());
		}

		private void unregisterRelation(Relation relation) {
			unregisterFeatureMap(relation.getFeatures());
		}

		private void registerFeatureMap(FeatureMap featureMap) {
			featureMap.addFeatureMapListener(this);
		}

		private void unregisterFeatureMap(FeatureMap featureMap) {
			featureMap.removeFeatureMapListener(this);
		}

		@Override
		public void featureMapUpdated() {
			changed = true;
		}

		@Override
		public void relationAdded(RelationSetEvent e) {
			changed = true;
			registerRelation(e.getRelation());
		}

		@Override
		public void relationRemoved(RelationSetEvent e) {
			changed = true;
			unregisterRelation(e.getRelation());
		}

		@Override
		public void annotationAdded(AnnotationSetEvent e) {
			changed = true;
			registerAnnotation(e.getAnnotation());
		}

		@Override
		public void annotationRemoved(AnnotationSetEvent e) {
			changed = true;
			unregisterAnnotation(e.getAnnotation());
		}

		@Override
		public void annotationSetAdded(DocumentEvent e) {
			changed = true;
			AnnotationSet annotationSet = document.getAnnotations(e.getAnnotationSetName());
			registerAnnotationSet(annotationSet);
		}

		@Override
		public void annotationSetRemoved(DocumentEvent e) {
			changed = true;
			AnnotationSet annotationSet = annotationSets.get(e.getAnnotationSetName());
			unregisterAnnotationSet(annotationSet);
		}

		@Override
		public void contentEdited(DocumentEvent e) {
			changed = true;
		}

		@Override
		public void annotationUpdated(AnnotationEvent e) {
			changed = true;
		}

	}

	protected final boolean hasDocumentChanged(Document document) {
		return documentChangeObservers.get(document).changed;
	}

	private final void unload() {
		unloaded = true;
	}

	/**
	 * loads the size of the virtual corpus
	 * 
	 * @return an Integer of the size
	 */
	protected abstract int loadSize() throws Exception;

	/**
	 * @param index of document in corpus
	 * @return the document name
	 */
	protected abstract String loadDocumentName(int index) throws Exception;

	/**
	 * @param index of document in corpus
	 * @return the document with features
	 */
	protected abstract Document loadDocument(int index) throws Exception;

	/**
	 * 
	 * @param index     where to insert new documents
	 * @param documents to add
	 * @return the amount of documents added
	 */
	protected abstract void addDocuments(int index, Collection<? extends Document> documents) throws Exception;

	protected abstract void setDocument(int index, Document document) throws Exception;

	protected void saveDocument(Document document) throws Exception {
		int index = this.indexOf(document);

		setDocument(index, document);
	}

	protected abstract void deleteDocuments(Collection<? extends Document> documents) throws Exception;

	protected abstract void deleteAllDocuments() throws Exception;

	protected abstract void renameDocument(Document document, String oldName, String newName) throws Exception;

	protected void documentUnloaded(int index, Document document) {
	}

	protected final void documentNameLoaded(int index, String documentName) {
		checkIndex(index);
		if (cacheDocumentNames != null && cacheDocumentNames > 0) {
			if (loadedDocumentNames.containsKey(index)) {
				if (!documentName.contentEquals(this.getDocumentName(index))) {
					throw new IllegalArgumentException("document name already loaded" + index + " and different");
				}
			}
			loadedDocumentNames.put(index, documentName);
			updateLruDocumentNameIndex(index);
		}
	}

	private void updateLruDocumentNameIndex(Integer index) {
		lruDocumentNameIndexes.remove(index);
		if (lruDocumentNameIndexes.size() >= cacheDocumentNames) {
			Iterator<Integer> iterator = lruDocumentNameIndexes.iterator();
			if (iterator.hasNext()) {
				Integer leastUsedIndex = iterator.next();
				iterator.remove();
				loadedDocumentNames.remove(leastUsedIndex);
			}
		}
		lruDocumentNameIndexes.add(index);
	}

	protected final void documentLoaded(int index, Document document) {
		checkIndex(index);
		if (loadedDocuments.containsKey(index)) {
			throw new IllegalArgumentException("document index already loaded: " + index);
		}
		if (document != null) {
			if (this.contains(document)) {
				throw new IllegalArgumentException(
						"document already loaded " + document + " at another index " + indexOf(document));
			}
			if (this.isDocumentNameLoaded(index)) {
				if (!document.getName().contentEquals(this.getDocumentName(index))) {
					throw new IllegalArgumentException("document name already loaded" + index + " and different");
				}
			} else {
				documentNameLoaded(index, document.getName());
			}
		}
		loadedDocuments.put(index, document);
		documentChangeObservers.put(document, new DocumentChangeObserver(document));

	}

	@Override
	public final boolean isDocumentLoaded(int index) {
		checkLoaded();
		checkIndex(index);
		return loadedDocuments.containsKey(index);
	}

	public final boolean isDocumentNameLoaded(int index) {
		checkLoaded();
		checkIndex(index);
		return loadedDocumentNames.containsKey(index);
	}

	@Override
	public final void unloadDocument(Document document) {
		checkLoaded();
		if (document == null) {
			return;
		}
		if (this.contains(document)) {
			if (!readonlyDocuments && hasDocumentChanged(document)) {
				try {
					saveDocument(document);
				} catch (Exception e) {
					throw new GateRuntimeException("cannot update document " + document, e);
				}
			}
			int index = this.indexOf(document);
			loadedDocuments.remove(index);
			documentChangeObservers.remove(document).unregisterDocument();
			documentUnloaded(index, document);
		}
	}

	@Override
	public final List<String> getDocumentNames() {
		checkLoaded();
		List<String> documentNames = new ArrayList<>();
		for (int i = 0; i < size(); i++) {
			String documentName = getDocumentName(i);
			documentNames.add(documentName);
		}
		return documentNames;
	}

	@Override
	public final String getDocumentName(int index) {
		checkLoaded();
		checkIndex(index);
		if (loadedDocumentNames.containsKey(index)) {
			updateLruDocumentNameIndex(index);
			return loadedDocumentNames.get(index);
		}

		String documentName;
		try {
			documentName = loadDocumentName(index);
		} catch (Exception e) {
			throw new GateRuntimeException("cannot load document name " + index, e);
		}
		documentNameLoaded(index, documentName);
		return documentName;
	}

	@Override
	public final Document get(int index) {
		checkLoaded();
		checkIndex(index);
		if (loadedDocuments.containsKey(index)) {
			return loadedDocuments.get(index);
		}

		Document document;
		try {
			document = loadDocument(index);
			if (document.getFeatures().getOrDefault("gate.SourceURL", "created from String")
					.equals("created from String")) {
				document.getFeatures().put("gate.SourceURL", "created from " + this.getClass().getSimpleName());
			}
		} catch (Exception e) {
			throw new GateRuntimeException("cannot load document " + index, e);
		}

		documentLoaded(index, document);
		return document;
	}

	@Override
	public final int size() {
		checkLoaded();
		if (size == null) {
			try {
				size = loadSize();
			} catch (Exception e) {
				throw new GateRuntimeException("cannot load corpus size", e);
			}
		}
		return size;
	}

	@Override
	public final boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public final int indexOf(Object object) {
		checkLoaded();
		if (object instanceof Document) {
			return loadedDocuments.entrySet().stream().filter(e -> e.getValue().equals(object)).map(e -> e.getKey())
					.min(Integer::compareTo).orElse(-1);
		}
		return -1;
	}

	@Override
	public final int lastIndexOf(Object object) {
		checkLoaded();
		if (object instanceof Document) {
			return loadedDocuments.entrySet().stream().filter(e -> e.getValue().equals(object)).map(e -> e.getKey())
					.max(Integer::compareTo).orElse(-1);
		}
		return -1;
	}

	@Override
	public final boolean contains(Object object) {
		checkLoaded();
		if (object instanceof Document) {
			return loadedDocuments.containsValue(object);
		}
		return false;
	}

	@Override
	public final boolean containsAll(Collection<?> collection) {
		checkLoaded();
		return loadedDocuments.values().containsAll(collection);
	}

	@Override
	public final boolean add(Document document) {
		addAll(size(), toList(document));
		return true;
	}

	@Override
	public final void add(int index, Document document) {
		addAll(index, toList(document));
	}

	@Override
	public final boolean addAll(Collection<? extends Document> documents) {
		return addAll(size(), documents);
	}

	@Override
	public final boolean addAll(int index, Collection<? extends Document> documents) {
		checkMutable();
		checkLoaded();
		checkIndexAdd(index);
		if (documents.isEmpty()) {
			return false;
		}
		size();

		try {
			addDocuments(index, documents);
		} catch (Exception e) {
			throw new GateRuntimeException("cannot add documents " + index + " " + documents, e);
		}

		shiftIndexMap(loadedDocumentNames, index, documents.size());
		addAllToIndexMap(loadedDocuments, index, documents);
		size += documents.size();
		modCount++;

		for (Document document : documents) {
			fireDocumentAdded(index++, document);
			documentChangeObservers.put(document, new DocumentChangeObserver(document));
		}
		return true;
	}

	@Override
	public final Document set(int index, Document document) {
		checkMutable();
		checkLoaded();
		checkIndex(index);

		try {
			setDocument(index, document);
		} catch (Exception e) {
			throw new GateRuntimeException("cannot set document " + index + " " + document, e);
		}

		removeFromIndexMap(loadedDocumentNames, index);
		Document oldDocument = this.loadedDocuments.put(index, document);
		fireDocumentRemoved(index, oldDocument);
		fireDocumentAdded(index, document);
		return oldDocument;
	}

	@Override
	public final Document remove(int index) {
		checkMutable();
		checkLoaded();
		checkIndex(index);
		size();
		Document document = get(index);

		try {
			deleteDocuments(toList(document));
		} catch (Exception e) {
			throw new GateRuntimeException("cannot delete document " + index + " " + document, e);
		}

		removeFromIndexMap(loadedDocumentNames, index);
		Document oldDocument = removeFromIndexMap(loadedDocuments, index);
		size--;
		modCount++;

		fireDocumentRemoved(index, oldDocument);
		return document;
	}

	@Override
	public final boolean remove(Object object) {
		checkMutable();
		checkLoaded();
		if (!this.contains(object)) {
			return false;
		}
		int index = this.indexOf(object);
		remove(index);
		return true;

	}

	@Override
	public final boolean removeAll(Collection<?> collection) {
		checkMutable();
		checkLoaded();
		Collection<Document> removeDocuments = new HashSet<>(this.loadedDocuments.values());
		removeDocuments.retainAll(collection);
		if (removeDocuments.isEmpty()) {
			return false;
		}
		size();

		try {
			deleteDocuments(removeDocuments);
		} catch (Exception e) {
			throw new GateRuntimeException("cannot delete documents " + removeDocuments, e);
		}

		for (Document document : removeDocuments) {
			while (this.contains(document)) {
				int index = this.lastIndexOf(document);
				removeFromIndexMap(loadedDocumentNames, index);
				Document oldDocument = removeFromIndexMap(loadedDocuments, index);
				fireDocumentRemoved(index, oldDocument);
				documentChangeObservers.remove(document).unregisterDocument();
			}
		}
		size -= removeDocuments.size();
		modCount++;

		return true;
	}

	@Override
	public final boolean retainAll(Collection<?> collection) {
		checkMutable();
		checkLoaded();
		Collection<Document> removeDocuments = new HashSet<>(this.loadedDocuments.values());
		removeDocuments.removeAll(collection);
		if (removeDocuments.isEmpty()) {
			return false;
		}
		size();

		try {
			deleteDocuments(removeDocuments);
		} catch (Exception e) {
			throw new GateRuntimeException("cannot delete documents " + removeDocuments, e);
		}

		for (Document document : removeDocuments) {
			while (this.contains(document)) {
				int index = this.lastIndexOf(document);
				removeFromIndexMap(loadedDocumentNames, index);
				Document oldDocument = removeFromIndexMap(loadedDocuments, index);
				fireDocumentRemoved(index, oldDocument);
				documentChangeObservers.remove(document).unregisterDocument();
			}
		}
		size -= removeDocuments.size();
		modCount++;

		return true;
	}

	@Override
	public final void clear() {
		checkMutable();
		checkLoaded();

		try {
			deleteAllDocuments();
		} catch (Exception e) {
			throw new GateRuntimeException("cannot delete all documents", e);
		}

		TreeMap<Integer, Document> reversed = new TreeMap<>(Collections.reverseOrder());
		reversed.putAll(loadedDocuments);

		loadedDocuments.clear();
		loadedDocumentNames.clear();

		for (Entry<Integer, Document> entry : reversed.entrySet()) {
			Integer index = entry.getKey();
			Document document = entry.getValue();
			fireDocumentRemoved(index, document);
			documentChangeObservers.remove(document).unregisterDocument();
		}
		size = 0;
		modCount++;
	}

	private static final <E> boolean addAllToIndexMap(SortedMap<Integer, E> map, Integer index,
			Collection<? extends E> c) {
		if (c.isEmpty()) {
			return false;
		}
		shiftIndexMap(map, index, c.size());
		for (E e : c) {
			map.put(index++, e);
		}
		return true;
	}

	private static final <E> E removeFromIndexMap(SortedMap<Integer, E> map, Integer index) {
		E old = map.remove(index);
		shiftIndexMap(map, index + 1, -1);
		return old;
	}

	private static final <E> void shiftIndexMap(SortedMap<Integer, E> map, Integer from, Integer shift) {
		Map<Integer, E> oldValues = map.tailMap(from);
		Map<Integer, E> movedValues = oldValues.entrySet().stream()
				.collect(Collectors.toMap(x -> x.getKey() + shift, x -> x.getValue()));
		map.keySet().removeAll(oldValues.keySet());
		map.putAll(movedValues);
	}

	private void checkLoaded() {
		if (unloaded) {
			throw new IllegalStateException("corpus is unloaded");
		}
	}

	private void checkMutable() {
		if (immutableCorpus) {
			throw new IllegalStateException("corpus is immutable");
		}
	}

	private void checkIndex(int index) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
		}
	}

	private void checkIndexAdd(int index) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
		}
	}

	@Override
	public final List<Document> subList(int fromIndex, int toIndex) {
		checkLoaded();
		throw new UnsupportedOperationException();
	}

	@Override
	public final Object[] toArray() {
		checkLoaded();
		Object[] array = new Object[size()];
		for (int i = 0; i < size(); i++) {
			Document document = get(i);
			array[i] = document;
		}
		return array;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
		checkLoaded();
		if (a.length < size)
			a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

		Object[] result = a;
		for (int i = 0; i < size(); i++) {
			result[i] = get(i);
		}

		if (a.length > size)
			a[size] = null;

		return a;
	}

	@Override
	public final Iterator<Document> iterator() {
		checkLoaded();
		return new VirtualCorpusIterator(this);
	}

	@Override
	public final ListIterator<Document> listIterator(int i) {
		checkLoaded();
		return new VirtualCorpusListIterator(this, i);
	}

	@Override
	public final ListIterator<Document> listIterator() {
		checkLoaded();
		return listIterator(0);
	}

	private static class VirtualCorpusIterator implements Iterator<Document> {
		protected VirtualCorpus corpus;
		protected int cursor = 0;
		protected int lastRet = -1;
		protected int expectedModCount;

		public VirtualCorpusIterator(VirtualCorpus corpus) {
			this.corpus = corpus;
			this.expectedModCount = corpus.modCount;
		}

		@Override
		public boolean hasNext() {
			return cursor != corpus.size();
		}

		@Override
		public Document next() {
			checkForComodification();
			try {
				int i = cursor;
				Document next = corpus.get(i);
				lastRet = i;
				cursor = i + 1;
				return next;
			} catch (IndexOutOfBoundsException e) {
				checkForComodification();
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			if (lastRet < 0)
				throw new IllegalStateException();
			checkForComodification();

			try {
				corpus.remove(lastRet);
				if (lastRet < cursor)
					cursor--;
				lastRet = -1;
				expectedModCount = corpus.modCount;
			} catch (IndexOutOfBoundsException e) {
				throw new ConcurrentModificationException();
			}
		}

		protected final void checkForComodification() {
			if (corpus.modCount != expectedModCount)
				throw new ConcurrentModificationException();
		}
	}

	private static class VirtualCorpusListIterator extends VirtualCorpusIterator implements ListIterator<Document> {

		public VirtualCorpusListIterator(VirtualCorpus corpus, int index) {
			super(corpus);
			this.cursor = index;
		}

		@Override
		public boolean hasPrevious() {
			return cursor != 0;
		}

		@Override
		public Document previous() {
			checkForComodification();
			try {
				int i = cursor - 1;
				Document previous = corpus.get(i);
				lastRet = cursor = i;
				return previous;
			} catch (IndexOutOfBoundsException e) {
				checkForComodification();
				throw new NoSuchElementException();
			}
		}

		@Override
		public int nextIndex() {
			return cursor;
		}

		@Override
		public int previousIndex() {
			return cursor - 1;
		}

		@Override
		public void set(Document e) {
			if (lastRet < 0)
				throw new IllegalStateException();
			checkForComodification();

			try {
				corpus.set(lastRet, e);
				expectedModCount = corpus.modCount;
			} catch (IndexOutOfBoundsException ex) {
				throw new ConcurrentModificationException();
			}
		}

		@Override
		public void add(Document e) {
			checkForComodification();

			try {
				int i = cursor;
				corpus.add(i, e);
				lastRet = -1;
				cursor = i + 1;
				expectedModCount = corpus.modCount;
			} catch (IndexOutOfBoundsException ex) {
				throw new ConcurrentModificationException();
			}
		}

	}

	private List<CorpusListener> corpusListeners = new ArrayList<CorpusListener>();

	@Override
	public void removeCorpusListener(CorpusListener listener) {
		corpusListeners.remove(listener);
	}

	@Override
	public void addCorpusListener(CorpusListener listener) {
		corpusListeners.add(listener);
	}

	protected void fireDocumentAdded(int index, Document document) {
		CorpusEvent event = new CorpusEvent(this, document, index, CorpusEvent.DOCUMENT_ADDED);
		for (CorpusListener listener : corpusListeners) {
			listener.documentAdded(event);
		}
	}

	protected void fireDocumentRemoved(int index, Document document) {
		CorpusEvent event = new CorpusEvent(this, document, index, CorpusEvent.DOCUMENT_REMOVED);
		for (CorpusListener listener : corpusListeners) {
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

}
