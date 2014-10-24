/*
 *  DirectoryCorpus.java
 *
 *
 * Copyright (c) 2010, Austrian Research Institute for
 * Artificial Intelligence (OFAI)
 *
 * This file is free
 * software, licenced under the GNU General Public License, Version 2
 *
 *  Johann Petrak, 30/8/2010
 *
 *  $Id: DirectoryCorpus.java 124 2014-04-24 18:23:51Z johann.petrak $
 */

package at.ofai.gate.virtualcorpus;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Properties;
import java.util.Collection;

import gate.*;
import gate.corpora.DocumentImpl;
import gate.creole.*;
import gate.creole.metadata.*;
import gate.event.CorpusEvent;
import gate.event.CorpusListener;
import gate.event.CreoleEvent;
import gate.event.CreoleListener;
import gate.persist.PersistenceException;
import gate.util.*;
import gate.util.persistence.PersistenceManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;

// TODO: use DocumentFormat.getSupportedFileSuffixes() to get the list of 
// supported input file extensions, unless the user limits those through 
// a parameter. 
// If we enable gzip-compression then we also add all the above extensions
// with .gz appended. 
// We allow to save back the files in the following formats: .xml .xml.gz and,
// if the plugin is loaded and finf is supported, finf. 
// QUESTION: is it possible to use a runtime-generated list as a default list
// for an init parameter to choose from / correct?

// BIGGER change: by default only support formats which can be written back,
// which would be xml, xml.gz and finf. In that case we may want to just 
// write back to the same file as we read from, no matter what.
// But if additional read/only extensions are specified, then we may want 
// to give the format to use for writing back? 
// OR: create different directory corpora: read only corpus which supports 
// all formats, but must save with "Save" option and ReadWrite corpus which 
// only supports the formats which can be used both for reading and writing.
// We could merge in the code from convertFormat to seperate out the 
// format conversion functionality!



/** 
 * A Corpus LR that mirrors files in a directory. In the default configuration,
 * just the <code>directoryURL</code> parameter is specified at creation and
 * all files that have a file extension of ".xml" and are not hidden are
 * accessible as documents through that corpus and automatically written back
 * to the directory when sync'ed or when unloaded (which does an implicit sync).
 * If the parameter <code>outDirectoryURL</code>
 * is also specified, the corpus reflects all the files from the 
 * <code>directoryURL</code> directory but writes any changed documents into
 * the directory <code>outDirectoryURL</code>. If the parameter 
 * <code>saveDocuments</code> is set to false, nothing is ever written
 * to either of the directories.
 * <p>
 * The main purpose of this Corpus implementation is that through it
 * a serial controller
 * can directly read and write from files stored in a directory. 
 * This makes it much easier to share working pipelines between pipeline
 * developers, especially when the pipeline files are checked into SCS.
 * <p>
 * This LR does not implement the following methods: 
 * <ul>
 * <li>toArray: none of the toArray methods is implemented. 
 * </ul>
 * If the parameter "transientCorpus" is false,
 * this corpus LR automatically uses a "dummy datastore" internally.
 * This datastore is created and removed automatically when the corpus LR is
 * created and removed. This datastore cannot be used for anything useful, it
 * does not allow listing of resources or storing of anything but documents
 * that are already in the corpus. It is mainly here because GATE assumes that
 * documents are either transient or from a datastore. To avoid documents from
 * a DirectoryCorpus to get treated as transient documents, their DataStore is
 * set to this dummy DataStore.
 * <p>
 * Documents will always get saved to either the original file or to a file
 * in the outDocumentURL directory whenever the document is synced or unloaded.
 * <p>
 * NOTE: If you use the "Save as XML" option from the LR's context menu, be
 * careful not specify the directory where the corpus saves documents as 
 * the target directory for the "Save as XML" function -- this might produce
 * unexpected results. Even if a different directory is specified, the 
 * "Save as XML" function will still also re-save the documents in the 
 * corpus directory unless the <code>saveDocuments</code> option is set to 
 * false.
 * 
 * @author Johann Petrak
 */
@CreoleResource(
    name = "DirectoryCorpus",
    interfaceName = "gate.Corpus", 
    icon = "corpus", 
    helpURL = "http://code.google.com/p/gateplugin-virtualcorpus/wiki/DirectoryCorpusUsage",
    comment = "A corpus backed by GATE documents in a file directory")
public class DirectoryCorpus  
  extends VirtualCorpus
  implements CreoleListener
  {

  //*****
  // Fields
  //******
  
  /**
   * 
   */
  private static final long serialVersionUID = -8485161260415382902L;

  // for accessing document name by index
  protected List<String> documentNames = new ArrayList<String>();
  // for checking if ith document is loaded
  protected List<Boolean> isLoadeds = new ArrayList<Boolean>();
  // for finding index for document name
  //REMOVE Map<String,Integer> documentIndexes = new HashMap<String,Integer>();
  
  protected Map<String,Document> loadedDocuments = new HashMap<String,Document>();
  
  protected File backingDirectoryFile;
  
  protected File outDirectoryFile;
  
  protected List<CorpusListener> listeners = new ArrayList<CorpusListener>();
  
  protected class OurFilenameFilter implements FilenameFilter {
    @Override
    public boolean accept(File directory, String filename) {
      if(outDirectoryURL != null) {
        return isValidDocumentName(filename,false,getUseCompression());
      } else {
        return isValidDocumentName(filename,true,getUseCompression());
      }
    }
  }
  
  Pattern docNamePatternXmlYesCompressionYes = 
    Pattern.compile("^[^.][^/\\*\\?\"<>|:]+\\.[Xx][Mm][Ll]\\.gz$");
  Pattern docNamePatternXmlNoCompressionYes = 
    Pattern.compile("^[^.][^/\\*\\?\"<>|:]+\\.gz$");
  Pattern docNamePatternXmlYesCompressionNo = 
    Pattern.compile("^[^.][^/\\*\\?\"<>|:]+\\.[Xx][Mm][Ll]$");
  Pattern docNamePatternXmlNoCompressionNo = 
    Pattern.compile("^[^.][^/\\*\\?\"<>|:]+$");
  Pattern docNamePatternFinf =
    Pattern.compile("^[^.][^/\\*\\?\"<>|:]+\\.[Ff][Ii][Nn][Ff$");
  
  
  //***************
  // Parameters
  //***************
  
  /**
   * Setter for the <code>directoryURL</code> LR initialization parameter.
   * @param dirURL The URL of the directory where the files for the corpus will
   * be read
   * from. If the <code>outDirectoryURL</code> is left empty the documents
   * will be written back to the original files in this directory when
   * unloaded (except when <code>saveDocuments</code> is set to false).
   */
  @CreoleParameter(comment = "The directory URL where files will be read from")
  public void setDirectoryURL(URL dirURL) {
    this.directoryURL = dirURL;
  }
  /**
   * Getter for the <code>directoryURL</code> LR initialization parameter.
   *
   * @return The directory URL where files are read from and (and saved to
   * if unloaded when outDirectoryURL is not specified and saveDocuments
   * is true).
   */
  public URL getDirectoryURL() {
    return this.directoryURL;
  }
  protected URL directoryURL = null;

  /**
   * Setter for the <code>outDirectoryURL</code> LR initialization parameter.
   *
   * @param dirURL The URL of a directory where modfied documents are stored.
   * If this is empty then the directoryURL will be used for both reading
   * and storing files. If this is provided, the files from the directoryURL
   * will not be overwritten and can have file extensions other than ".xml"
   * of no file extension at all. The files writting in this directory will
   * always have their file extension set to ".xml" either by replacing an
   * existing extension or appending it.
   * <p/>
   * NOTE: Any existing files in this directory can be overwritten. Also, if
   * the directoryURL contains several files which only differ in their
   * file extension, they will all be written to the same file with extension
   * ".xml".
   * <p/>
   * NOTE: this LR does not allow multi-threaded use! It does NOT allow being
   * used for getting or saving documents and getting serialized (saved as
   * part of a .gapp file) at the same time!
   */
  @Optional
  @CreoleParameter(
    comment = "The directory URL where files will be written to. "+
              "If missing same as directoryURL")
  public void setOutDirectoryURL(URL dirURL) {
    this.outDirectoryURL = dirURL;
  }
  /**
   * Getter for the <code>outDirectoryURL</code> LR initialization parameter.
   *
   * @return the URL where documents are saved as GATE XML files when unloaded.
   */
  public URL getOutDirectoryURL() {
    return this.outDirectoryURL;
  }
  protected URL outDirectoryURL;

  @Optional
  @CreoleParameter(
    comment = "The MIME type to use; if left blank some MIME type is guessed",
    defaultValue = "")
  public void setMimeType(String value) {
    mimeType = value;
  }
  public String getMimeType() {
    return mimeType;
  }
  protected String mimeType = "";
  
  @Optional
  @CreoleParameter(
    comment = "The encoding to use; if left blank the default encoding used by GATE is used",
    defaultValue = "")
  public void setEncoding(String value) {
    encoding = value;
  }
  public String getEncoding() {
    return encoding;
  }
  protected String encoding = "";
  
  DummyDataStore4DirCorp ourDS = null;

  /**
   * Initializes the DirectoryCorpus LR
   * @return 
   * @throws ResourceInstantiationException
   */
  @Override
  public Resource init() 
    throws ResourceInstantiationException {
    if(directoryURL == null) {
      throw new ResourceInstantiationException("directoryURL must be set");
    }
    if(outDirectoryURL == null) {
      outDirectoryFile = Files.fileFromURL(directoryURL);
    } else {
      outDirectoryFile = Files.fileFromURL(outDirectoryURL);
    }
    backingDirectoryFile = Files.fileFromURL(directoryURL);
    try {
      backingDirectoryFile = backingDirectoryFile.getCanonicalFile();
    } catch (IOException ex) {
      throw new ResourceInstantiationException(
              "Cannot get canonical file for "+backingDirectoryFile,ex);
    }
    try {
      outDirectoryFile = outDirectoryFile.getCanonicalFile();
    } catch (IOException ex) {
      throw new ResourceInstantiationException(
              "Cannot get canonical file for "+outDirectoryFile,ex);
    }
    if(!backingDirectoryFile.isDirectory()) {
      throw new ResourceInstantiationException(
              "Not a directory "+backingDirectoryFile);
    }
    if(!outDirectoryFile.isDirectory()) {
      throw new ResourceInstantiationException(
              "Not a directory "+outDirectoryFile);
    }
    File[] files = backingDirectoryFile.listFiles(new OurFilenameFilter());
    int i = 0;
    if(files != null) {
      for(File file : files) {
        String filename = file.getName();
        documentNames.add(filename);
        isLoadeds.add(false);
        documentIndexes.put(filename, i);
        i++;
      }
    }
    try {
      PersistenceManager.registerPersistentEquivalent(
          at.ofai.gate.virtualcorpus.DirectoryCorpus.class,
          at.ofai.gate.virtualcorpus.DirectoryCorpusPersistence.class);
    } catch (PersistenceException e) {
      throw new ResourceInstantiationException(
              "Could not register persistence",e);
    }
    if (!isTransientCorpus) {
      try {
        ourDS =
          (DummyDataStore4DirCorp) Factory.createDataStore("at.ofai.gate.virtualcorpus.DummyDataStore4DirCorp", backingDirectoryFile.getAbsoluteFile().toURI().toURL().toString());
        ourDS.setName("DummyDS4_" + this.getName());
        ourDS.setComment("Dummy DataStore for DirectoryCorpus " + this.getName());
        ourDS.setCorpus(this);
        //System.err.println("Created dummy corpus: "+ourDS+" with name "+ourDS.getName());
      } catch (Exception ex) {
        throw new ResourceInstantiationException(
          "Could not create dummy data store", ex);
      }
    }
    Gate.getCreoleRegister().addCreoleListener(this);
    return this;
  }
  
  /**
   * Test is the document with the given index is loaded. If an index is 
   * specified that is not in the corpus, a GateRuntimeException is thrown.
   * 
   * @param index 
   * @return true if the document is loaded, false otherwise. 
   */
  public boolean isDocumentLoaded(int index) {
    if(index < 0 || index >= isLoadeds.size()) {
      throw new GateRuntimeException("Document number "+index+
              " not in corpus "+this.getName()+" of size "+isLoadeds.size());
    }
    //System.out.println("isDocumentLoaded called: "+isLoadeds.get(index));
    return isLoadeds.get(index);
  }

  public boolean isDocumentLoaded(Document doc) {
    String docName = doc.getName();
    //System.out.println("DirCorp: called unloadDocument: "+docName);
    Integer index = documentIndexes.get(docName);
    if(index == null) {
      throw new RuntimeException("Document "+docName+
              " is not contained in corpus "+this.getName());
    }
    return isDocumentLoaded(index);
  }

  /**
   * Unload a document from the corpus. When a document is unloaded it
   * is automatically stored in GATE XML format to the directory where it
   * was read from or to the directory specified for the outDirectoryURL
   * parameter. If saveDocuments is false, nothing is saved at all.
   * If the document is not part of the corpus, a GateRuntimeException is
   * thrown.
   *
   * @param doc
   */
  public void unloadDocument(Document doc) {
    String docName = doc.getName();
    //System.out.println("DirCorp: called unloadDocument: "+docName);
    Integer index = documentIndexes.get(docName);
    if(index == null) {
      throw new RuntimeException("Document "+docName+
              " is not contained in corpus "+this.getName());
    }
    if(isDocumentLoaded(index)) {
      // if saveOnUnload is set, save the document
      if(saveDocuments) {
        saveDocument(doc);
      }
      loadedDocuments.remove(docName);
      isLoadeds.set(index, false);
      //System.err.println("Document unloaded: "+docName);
    } // else silently do nothing
  }
  
  
  public void removeCorpusListener(CorpusListener listener) {
    listeners.remove(listener);
  }
  public void addCorpusListener(CorpusListener listener) {
    listeners.add(listener);
  }

  /**
   * Get the list of document names in this corpus.
   *
   * @return the list of document names 
   */
  public List<String> getDocumentNames() {
    List<String> newList = new ArrayList<String>(documentNames);
    return newList;
  }

  /**
   * Return the name of the document with the given index from the corpus. 
   *
   * @param i the index of the document to return
   * @return the name of the document with the given index
   */
  public String getDocumentName(int i) {
    return documentNames.get(i);
  }

  /**
   * @return
   */
  public DataStore getDataStore() {
    if(dataStoreIsHidden) {
      return null;
    } else {
      return ourDS;
    }
  }

  /**
   * This always throws a PersistenceException as this kind of corpus cannot
   * be saved to a datastore.
   * 
   * @param ds
   * @throws PersistenceException
   */
  @Override
  public void setDataStore(DataStore ds) throws PersistenceException {
    throw new PersistenceException("Corpus "+this.getName()+
            " cannot be saved to a datastore");
  }

  /**
   * This follows the convention for transient corpus objects and always
   * returns false.
   * 
   * @return always false
   */
  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public void sync() {
    // TODO: save the document!?!?
  }


  @Override
  public void cleanup() {
    // TODO:
    // deregister our listener for resources of type document
    //
    if(!isTransientCorpus) {
      Gate.getDataStoreRegister().remove(ourDS);
    }
  }

  @Override
  public void setName(String name) {
    super.setName(name);
    if(ourDS != null) {
      ourDS.setName("DummyDS4_"+this.getName());
      ourDS.setComment("Dummy DataStore for DirectoryCorpus "+this.getName());
    }
  }


  // Methods to be implemented from List

  /**
   * Add a document to the corpus. If the document has a name that is already
   * in the list of documents, return false and do not add the document.
   * Note that only the name is checked!
   * If the name of the document added is not ending in ".xml", a 
   * GateRuntimeException is thrown.
   * If the document is already adopted by some data store throw an exception.
   */
  public boolean add(Document doc) {
    if(!saveDocuments) {
      return false;
    }
    //System.out.println("DocCorp: called add(Object): "+doc.getName());
    String docName = doc.getName();
    ensureValidDocumentName(docName,true);
    Integer index = documentIndexes.get(docName);
    if(index != null) {
      return false;  // if that name is already in the corpus, do not add
    } else {
      // for now, we do not allow any document to be added that is already
      // adopted by a datastore.
      if(doc.getDataStore() != null) {
        throw new GateRuntimeException("Cannot add "+doc.getName()+" which belongs to datastore "+doc.getDataStore().getName());
      }
      saveDocument(doc);
      int i = documentNames.size();
      documentNames.add(docName);
      documentIndexes.put(docName, i);
      isLoadeds.add(false);
      if(!isTransientCorpus) {
        adoptDocument(doc);
      }
      fireDocumentAdded(new CorpusEvent(
          this, doc, i, CorpusEvent.DOCUMENT_ADDED));
      
      return true;
    }
  }


  /**
   * This removes all documents from the corpus. Note that this does nothing
   * when the saveDocuments parameter is set to false.
   * If the outDirectoryURL parameter was set, this method will throw
   * a GateRuntimeException.
   */
  public void clear() {
    if(!saveDocuments) {
      return;
    }
    if(outDirectoryURL != null) {
      throw new GateRuntimeException(
              "clear method not supported when outDirectoryURL is set for "+
              this.getName());
    }
    for(int i=documentNames.size()-1; i>=0; i--) {
      remove(i);
    }
  }
  
  /**
   * This checks if a document with the same name as the document
   * passed is already in the corpus. The content is not considered 
   * for this.
   */
  public boolean contains(Object docObj) {
    Document doc = (Document)docObj;
    String docName = doc.getName();
    return (documentIndexes.get(docName) != null);
  }
  

  /**
   * Return the document for the given index in the corpus.
   * An IndexOutOfBoundsException is thrown when the index is not contained
   * in the corpus.
   * The document will be read from the file only if it is not already loaded.
   * If it is already loaded a reference to that document is returned.
   * 
   * @param index
   * @return 
   */
  public Document get(int index) {
    //System.out.println("DirCorp: called get(index): "+index);
    if(index < 0 || index >= documentNames.size()) {
      throw new IndexOutOfBoundsException(
          "Index "+index+" not in corpus "+this.getName()+
          " of size "+documentNames.size());
    }
    String docName = documentNames.get(index);
    if(isDocumentLoaded(index)) {
      Document doc = loadedDocuments.get(docName);
      //System.out.println("Returning loaded document "+doc);
      return doc;
    }
    //System.out.println("Document not loaded, reading");
    Document doc = readDocument(docName,getUseCompression());
    loadedDocuments.put(docName, doc);
    isLoadeds.set(index, true);
    if(!isTransientCorpus) {
      adoptDocument(doc);
    }
    return doc;
  }

  /**
   * Returns the index of the document with the same name as the given document
   * in the corpus. The content of the document is not considered for this.
   * 
   * @param docObj
   * @return
   */
  public int indexOf(Object docObj) {
    Document doc = (Document)docObj;
    String docName = doc.getName();
    Integer index = documentIndexes.get(docName);
    if(index == null) {
      return -1;
    } else {
      return index;
    }
  }

  /**
   * Check if the corpus is empty.
   *
   * @return true if the corpus is empty
   */
  public boolean isEmpty() {
    return (documentNames.isEmpty());
  }

  /**
   * Returns an iterator to iterate through the documents of the
   * corpus. The iterator does not allow modification of the corpus.
   * 
   * @return
   */
  public Iterator<Document> iterator() {
    return new DirectoryCorpusIterator();
  }

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   * 
   * @param docObj
   * @return
   */
  public int lastIndexOf(Object docObj) {
    throw new MethodNotImplementedException(
            notImplementedMessage("lastIndexOf(Object)"));
  }

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   * @return
   */
  public ListIterator<Document> listIterator() {
    throw new MethodNotImplementedException(
            notImplementedMessage("listIterator"));
  }

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   *
   * @param i
   * @return
   */
  public ListIterator<Document> listIterator(int i) {
    throw new MethodNotImplementedException(
            notImplementedMessage("listIterator(int)"));
  }

  /**
   * Removes the document with the given index from the corpus. This is not
   * supported and throws a GateRuntimeException if the outDirectoryURL
   * was specified for this corpus. If the saveDocuments parameter is false
   * for this corpus, this method does nothing.
   * A document which is removed from the corpus will have its dummy
   * datastore removed and look like a transient document again. 
   * 
   * @param index
   * @return the document that was just removed from the corpus
   */
  public Document remove(int index) {
    Document doc = (Document)get(index);
    String docName = documentNames.get(index);
    documentNames.remove(index);
    if(isLoadeds.get(index)) {
      loadedDocuments.remove(docName);
    }
    isLoadeds.remove(index);
    documentIndexes.remove(docName);
    removeDocument(docName);
    if (!isTransientCorpus) {
      try {
        doc.setDataStore(null);
      } catch (PersistenceException ex) {
        // this should never happen
      }
    }
    fireDocumentRemoved(new CorpusEvent(
        this, doc,
        index, CorpusEvent.DOCUMENT_REMOVED));
    return doc;
  }

  /**
   * Removes a document with the same name as the given document
   * from the corpus. This is not
   * supported and throws a GateRuntimeException if the outDirectoryURL
   * was specified for this corpus. If the saveDocuments parameter is false
   * for this corpus, this method does nothing and always returns false.
   * If the a document with the same name as the given document is not
   * found int the corpus, this does nothing and returns false.
   * 
   * @param docObj
   * @return true if a document was removed from the corpus
   */
  public boolean remove(Object docObj) {
    int index = indexOf(docObj);
    if(index == -1) {
      return false;
    }
    String docName = documentNames.get(index);
    documentNames.remove(index);
    isLoadeds.remove(index);
    documentIndexes.remove(docName);
    removeDocument(docName);  
    Document doc = isDocumentLoaded(index) ? (Document)get(index) : null;
    if (!isTransientCorpus) {
      try {
        doc.setDataStore(null);
      } catch (PersistenceException ex) {
        // this should never happen
      }
    }
    fireDocumentRemoved(new CorpusEvent(
        this, doc,
        index, CorpusEvent.DOCUMENT_REMOVED));
    return true;
  }

  /**
   * Remove all the documents in the collection from the corpus.
   *
   * @param coll
   * @return true if any document was removed
   */
  public boolean removeAll(Collection coll) {
    boolean ret = false;
    for(Object docObj : coll) {
      ret = ret || remove(docObj);
    }
    return ret;
  }

  
  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   * 
   * @param index
   * @param obj
   * @return
   */
  public Document set(int index, Document obj) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("set(int,Object)"));
  }
  
  public int size() {
    return documentNames.size();
  }

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   * @param i1
   * @param i2
   * @return
   */
  public List<Document> subList(int i1, int i2) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("subList(int,int)"));
  }


  
  //****** 
  //Listener methods
  //***********
  protected void fireDocumentAdded(CorpusEvent e) {
    for(CorpusListener listener : listeners) {
      listener.documentAdded(e);
    }
  }

  protected void fireDocumentRemoved(CorpusEvent e) {
    for(CorpusListener listener : listeners) {
      listener.documentRemoved(e);
    }
  }

  public void resourceLoaded(CreoleEvent e) {
  }

  public void resourceRenamed(
          Resource resource,
          String oldName,
          String newName) {
    // if one of our documents gets renamed, rename it back and
    // write an error message
    if(resource instanceof Document) {
      Document doc = (Document)resource;
      if(loadedDocuments.containsValue(doc)) {
        System.err.println("ERROR: documents from a directory corpus cannot be renamed!");
        doc.setName(oldName);
      }
    }
  }

  public void resourceUnloaded(CreoleEvent e) {
    Resource res = e.getResource();
    if(res instanceof Document) {
      Document doc = (Document)res;
      // check if this document has actually been loaded by us
      if(loadedDocuments.containsValue(doc)) {
        unloadDocument(doc);
      } // else: its not ours, ignore
    } else if(res == this) {
      Gate.getCreoleRegister().removeCreoleListener(this);
    }
  }

  public void datastoreClosed(CreoleEvent ev) {
  }
  
  public void datastoreCreated(CreoleEvent ev) {
    
  }
  
  public void datastoreOpened(CreoleEvent ev) {
    
  }
  
  //**************************
  // helper methods
  // ************************
  protected void saveDocument(Document doc) {
    //System.out.println("DirCorp: save doc "+doc.getName());
    // at this point the document should be checked to be a valid file name
    // If the document name does not end in ".xml" we either replace any
    // existing extension with ".xml" or append ".xml"
    if(!getSaveDocuments()) {
      return;
    }
    boolean compressOnCopy = getCompressOnCopy();
    boolean useCompression = getUseCompression();
    String docName = doc.getName();
    // Avoid adding additional .xml and/orf .gz endings if we already have
    // them: this is done by always removing any .xml or .gz that might
    // be there and then re-adding them as needed.
    docName = docName.replaceAll("\\.gz$", "");
    docName = docName.replaceAll("\\.xml$", "");
    docName += ".xml";
    if(compressOnCopy || useCompression) {
      docName += ".gz";
    }
    File docFile = new File(outDirectoryFile, docName);
    String xml = doc.toXml();
    if(compressOnCopy || useCompression) {
      String outputEncoding = "UTF-8";
      String sysEncoding = System.getProperty("file.encoding");
      if(encoding != null && !encoding.isEmpty()) {
        outputEncoding = encoding;
        //System.out.println("Encoding set to encoding parm: "+encoding);
      } else if(sysEncoding != null && !sysEncoding.isEmpty() ) {
        outputEncoding = sysEncoding;
        //System.out.println("Encoding set to system encoding: "+sysEncoding);
      }
      byte[] buf = null;
      try {
        buf = xml.getBytes(outputEncoding);
      } catch (UnsupportedEncodingException ex) {
        throw new GateRuntimeException("Could not convert to encoding: "+outputEncoding+", file "+docFile,ex);
      }
      OutputStream os;
      OutputStream ourOut;
      try {
        os = new FileOutputStream(docFile);
      } catch (FileNotFoundException ex) {
        throw new GateRuntimeException("File not found on writing but listed in corpus: "+docFile,ex);
      }
      try {
        ourOut = new GZIPOutputStream(os);
      } catch (IOException ex) {
        IOUtils.closeQuietly(os);
        throw new GateRuntimeException("IO exception when creating compressed strem for file "+docFile,ex);
      }
      try {
        ourOut.write(buf);
      } catch (IOException ex) {
        IOUtils.closeQuietly(ourOut);
        IOUtils.closeQuietly(os);
        throw new GateRuntimeException("IO exception when writing compressed stream for file "+docFile,ex);
      }
      try {
        ourOut.close();
      } catch (IOException ex) {
        IOUtils.closeQuietly(os);
        throw new GateRuntimeException("IO exception when closing compressed stream for file "+docFile,ex);
      }
      try {
        os.close();
      } catch (IOException ex) {
        throw new GateRuntimeException("IO exception when closing output stream for file "+docFile,ex);
      }
    } else {
      try {
        OutputStreamWriter writer =
          new OutputStreamWriter(new FileOutputStream(docFile));
        writer.write(xml);
        writer.flush();
        writer.close();
        //System.err.println("Document saved: "+docName);
      } catch (Exception ex) {
        throw new GateRuntimeException(
          "Could not write document " + doc.getName(), ex);
      }
    }
  }
  
  protected Document readDocument(String docName, boolean compression) {
    //System.out.println("DirCorp: read doc "+docName);
    File docFile = new File(backingDirectoryFile, docName);
    URL docURL;
    Document doc = null;
    try {
      docURL = docFile.toURI().toURL();
    } catch (MalformedURLException ex) {
      throw new GateRuntimeException(
              "Could not create URL for document name "+docName,ex);
    }
    FeatureMap params = Factory.newFeatureMap();
    if(mimeType != null && !mimeType.isEmpty()) {
      params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
    }
    if(encoding != null && !encoding.isEmpty()) {
      params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
    }
    if(compression) {
      // TODO: read from URL stream using a compression stream wrapper
      String content = null;
      InputStream isorig = null;
      try {
        isorig = new FileInputStream(docFile);
      } catch (FileNotFoundException ex) {
        throw new GateRuntimeException("Cannot find file though listed in corpus: "+docFile,ex);
      }
      InputStream isdec = null;
      try {
        isdec = new GZIPInputStream(isorig);
      } catch (IOException ex) {
        IOUtils.closeQuietly(isdec);
        IOUtils.closeQuietly(isorig);
        throw new GateRuntimeException("IO exception when opening decompress stream for file "+docFile,ex);
      }
      try {
        String usedEncoding = "UTF-8";
        if(encoding != null && !encoding.isEmpty()) {
          usedEncoding = encoding;
        } else if(System.getProperty("file.encoding") != null) {
          usedEncoding = System.getProperty("file.encoding");
        }
        content = IOUtils.toString(isdec, usedEncoding);
      } catch (IOException ex) {
        throw new GateRuntimeException("IO exception when reading compressed stream for file "+docFile,ex);
      }
      try {
        isdec.close();
      } catch (IOException ex) {
        throw new GateRuntimeException("IO Exception when closing compressed stream for file "+docFile,ex);
      }
      try {
        isorig.close();
      } catch (IOException ex) {
        throw new GateRuntimeException("IO Exception when closing file strem for file "+docFile,ex);
      }
      params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME,content);
      if(mimeType != null && !mimeType.isEmpty()) {
        params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME,mimeType);
      }
      if(encoding != null && !encoding.isEmpty()) {
        params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME,encoding);
      }
      try {
        doc =
          (Document) Factory.createResource(
            DocumentImpl.class.getName(),
            params, null, docName);
      } catch (ResourceInstantiationException ex) {
        throw new GateRuntimeException(
          "Could not create Document from loaded content from compressed file " + docFile, ex);
      }
    } else {
      params.put(Document.DOCUMENT_URL_PARAMETER_NAME, docURL);
      try {
        doc =
          (Document) Factory.createResource(DocumentImpl.class.getName(),
          params, null, docName);
      } catch (ResourceInstantiationException ex) {
        throw new GateRuntimeException(
          "Could not create Document from URL " + docURL, ex);
      }
    }
    return doc;
  }
  
  protected void removeDocument(String docName) {
    //System.out.println("DirCorp: remove doc "+docName);
    if(getOutDirectoryURL() != null) {
      return;
    }
    if(getRemoveDocuments() && getSaveDocuments()) {
      File docFile = new File(outDirectoryFile, docName);
      docFile.delete();
    } 
  }
  
  protected boolean isValidDocumentName(String docName, boolean onlyXML, boolean compress) {
    // this corpus only allows document names that are also valid file names
    // Names must not start with a dot.
    // If onlyXML is set, all names must end with ".xml"
    // Names must not be longer than 200 characters 
    // If compression is used, the name must end with ".gz"
    if(docName.length() > 200) {
      return false;
    }
    docNamePatternXmlNoCompressionNo.matcher(name).matches();
    if( (  onlyXML &&  compress && 
             !docNamePatternXmlYesCompressionYes.matcher(docName).matches() ) ||
        (  onlyXML && !compress && 
             !docNamePatternXmlYesCompressionNo.matcher(docName).matches() ) ||
        ( !onlyXML &&  compress && 
             !docNamePatternXmlNoCompressionYes.matcher(docName).matches() ) ||
        ( !onlyXML && !compress && 
             !docNamePatternXmlNoCompressionNo.matcher(docName).matches() ) )
    {
      return false;
    }
    return true;
  }
  
  protected void ensureValidDocumentName(String docName, boolean onlyXML) {
    if(!isValidDocumentName(docName, onlyXML,getUseCompression())) {
      throw new GateRuntimeException(
              "Not a valid document name for a DirectoryCorpus: "+docName);
    }
  }

  protected void adoptDocument(Document doc) {
    try {
      doc.setDataStore(ourDS);
      //System.err.println("Adopted document "+doc.getName());
    } catch (PersistenceException ex) {
      //System.err.println("Got exception when adopting: "+ex);
    }
  }
  
  protected class DirectoryCorpusIterator implements Iterator<Document> {
    int nextIndex = 0;
    @Override
    public boolean hasNext() {
      return (documentNames.size() > nextIndex);
    }
    @Override
    public Document next() {
      if(hasNext()) {
        return get(nextIndex++);
      } else {
        return null;
      }
    }
    @Override
    public void remove() {
      throw new MethodNotImplementedException();
    }    
  }


} // class DirectoryCorpus
