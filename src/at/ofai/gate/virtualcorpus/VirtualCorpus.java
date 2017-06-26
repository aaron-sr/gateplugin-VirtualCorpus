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


import gate.Corpus;
import gate.DataStore;
import gate.Document;
import gate.Gate;
import gate.Resource;
import gate.creole.*;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.HiddenCreoleParameter;
import gate.creole.metadata.Optional;
import gate.event.CorpusEvent;
import gate.event.CorpusListener;
import gate.event.CreoleEvent;
import gate.event.CreoleListener;
import gate.persist.PersistenceException;
import gate.util.GateRuntimeException;
import gate.util.MethodNotImplementedException;
import java.io.FileFilter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.log4j.Logger;

/** 
 * 
 * @author Johann Petrak
 */
public abstract class VirtualCorpus
  extends AbstractLanguageResource implements Corpus, CreoleListener
{

  /**
   * Setter for the <code>immutable</code> LR initialization parameter.
   *
   * @param immutable If set to true, the corpus list cannot be changed, i.e.
   * documents cannot be removed or deleted. All methods which would otherwise
   * change the corpus content are silently ignored.
   * 
   * NOTE: For now this is hidden and all instances of virtual corpora are
   * immutable! This parameter may get removed in the future and all VirtualCorpus
   * instances may forever remain immutable!
   *
   */
  @Optional
  @HiddenCreoleParameter
  @CreoleParameter(comment="if true, the corpus content cannot be changed, documents cannot be added or removed",
    defaultValue="true")
  public void setImmutable(Boolean immutable) {
    this.immutable = immutable;
  }
  public Boolean getRemoveDocuments() {
    return this.immutable;
  }
  protected Boolean immutable = true;

  /**
   * Setter for the <code>readonly</code> LR initialization parameter.
   *
   * @param readonly If set to true, documents will never be saved back. All
   * methods which would otherwise cause a document to get saved are silently
   * ignored.
   */
  @Optional
  @CreoleParameter(comment="If true, documents will never be saved",
    defaultValue="false")
  public void setReadonly(Boolean readonly) {
    this.readonly = readonly;
  }
  public Boolean getReadonly() {
    return this.readonly;
  }
  protected Boolean readonly = true;

  
  public void populate( // OK
      URL directory, FileFilter filter,
      String encoding, boolean recurseDirectories) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("populate(URL, FileFilter, boolean)"));
  } 


  
  public long populate(URL url, String docRoot, String encoding, int nrdocs,
      String docNamePrefix, String mimetype, boolean includeroot) {
    throw new gate.util.MethodNotImplementedException(
        notImplementedMessage("populate(URL, String, String, int, String, String, boolean"));
  }


   public void populate ( // OK
      URL directory, FileFilter filter,
      String encoding, String mimeType,
      boolean recurseDirectories) {
   throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("populate(URL, FileFilter, String, String, boolean"));
  }

  /**
   * @param trecFile
   * @param encoding
   * @param numberOfDocumentsToExtract
   * @return
   */
  public long populate(
      URL trecFile, String encoding, int numberOfDocumentsToExtract) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("populate(URL, String, int"));
  }


  protected String notImplementedMessage(String methodName) {
    return "Method "+methodName+" not supported for VirtualCorpus corpus "+
            this.getName()+" of class "+this.getClass();
  }

  /**
   * For mapping document names to document indices.
   */
  Map<String,Integer> documentIndexes = new HashMap<String,Integer>();

  // for accessing document name by index
  protected List<String> documentNames = new ArrayList<String>();
  // for checking if ith document is loaded
  protected List<Boolean> isLoadeds = new ArrayList<Boolean>();
  // for finding index for document name
  //REMOVE protected Map<String,Integer> documentIndexes = new HashMap<String,Integer>();
  
  protected Map<String,Document> loadedDocuments = new HashMap<String,Document>();
  
  private static Logger logger = Logger.getLogger(VirtualCorpus.class);
  
  protected DummyDataStore4Virtuals ourDS = null;  
  
  /**
   * Test is the document with the given index is loaded. If an index is 
   * specified that is not in the corpus, a GateRuntimeException is thrown.
   * 
   * @param index 
   * @return true if the document is loaded, false otherwise. 
   */
  @Override
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
   * Unload a document from the corpus. 
   * This mimics what SerialCorpusImpl does: the document gets synced which
   * in turn will save the document, then it gets removed from memory.
   * Syncing will make our dummy datastore to invoke our own saveDocument
   * method. The saveDocument method determines if the document should really
   * be saved and how.
   *
   * @param doc
   */
  @Override
  public void unloadDocument(Document doc) {
    unloadDocument(doc,true);
  }
  
  // NOTE: unfortunately this method, like the unloadDocument(int) methods
  // is not in the Corpus interface. 
  public void unloadDocument(Document doc, boolean sync) {
    String docName = doc.getName();
    logger.debug("DirectoryCorpus: called unloadDocument: "+docName);
    Integer index = documentIndexes.get(docName);
    if(index == null) {
      throw new RuntimeException("Document "+docName+
              " is not contained in corpus "+this.getName());
    }
    if(isDocumentLoaded(index)) {
      if(sync) { 
        try { 
          doc.sync();
        } catch (Exception ex) {
          throw new GateRuntimeException("Problem syncing document "+doc.getName(),ex);
        }
      }
      loadedDocuments.remove(docName);
      isLoadeds.set(index, false);
      //System.err.println("Document unloaded: "+docName);
    } // else silently do nothing
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
  @Override
  public String getDocumentName(int i) {
    return documentNames.get(i);
  }

  /**
   * @return
   */
  @Override
  public DataStore getDataStore() {
      return ourDS;
  }

  /**
   * This always throws a PersistenceException as this kind of corpus cannot
   * be saved to a new datastore.
   * 
   * @param ds
   * @throws PersistenceException
   */
  @Override
  public void setDataStore(DataStore ds) throws PersistenceException {
    // TODO: this oddly now gets invoked when trying to save a pipeline, so
    // instead of throwing an exception we just do nothing for now
    //throw new PersistenceException("Corpus "+this.getName()+
    //        " cannot be saved to a datastore");
    System.err.println("Invoked setDataStore(ds)");
  }

  /**
   * This returns false because the corpus itself cannot be in a modified,
   * unsaved state.
   * 
   * For now all DirectoryCorpus objects are immutable: the list of documents
   * cannot be changed. Therefore, there is no way to modfy the corpus LR.
   * However, even if documents can be added or removed at some point, 
   * these changes will be immediately reflected in the backing directory,
   * so there is no way to modify the corpus and not have these changes 
   * "saved" or "synced". 
   * The bottom line is that this will always return false.
   * 
   * @return always false
   */
  @Override
  public boolean isModified() {
    return false;
  }

  /**
   * Syncing the corpus does nothing.
   * For an immutable corpus, there is nothing that would ever need to 
   * get synced (saved) and for a mutable corpus, all changes are saved
   * immediately so "syncing" is never necessary.
   * NOTE: syncing the corpus itself does not affect and should not affect
   * any documents which still may be modified and not synced.
   */
  @Override
  public void sync() {
    // do nothing.
  }

  /**
   * Set the name of the DirectoryCorpus.
   * Note that this can be called by the factory before init is run!
   * @param name 
   */
  @Override
  public void setName(String name) {
    logger.info("VirtualCorpus: calling setName with "+name);
    super.setName(name);
    // If we get called before init, there will be no DS yet, so no need
    // to rename it!
    if(ourDS != null) {
      ourDS.setName("DummyDS4_"+this.getName());
      ourDS.setComment("Dummy DataStore for VirtualCorpus "+this.getName());
    }
  }

  protected abstract void saveDocument(Document doc);
  
  
  @Override
  public abstract Document get(int index);
  
  /**
   * This method is not implemented and throws 
   * a gate.util.MethodNotImplementedException
   * 
   * @param index
   * @param docObj
   */
  public void add(int index, Document docObj) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("add(int,Object)"));
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

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   * @return
   */
  public Object[] toArray() {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("toArray()"));
  }

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   * @return
   */
  public Object[] toArray(Object[] x) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("toArray()"));
  }
  
  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   * @param coll
   * @return
   */
  public boolean retainAll(Collection coll) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("retainAll(Collection)"));
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
  
  /**
   * Add all documents in the collection to the end of the corpus.
   * Documents with a name that is already in the corpus are not added.
   * 
   * @param c a collection of documents
   * @return true if any document from the corpus was added.
   */
  public boolean addAll(Collection c) {
    boolean ret = false;
    for(Object obj : c) {
      if(obj instanceof Document) {
        ret = ret || this.add((Document)obj);
      } 
    }
    return ret;
  }

  /**
   * Not implemented.
   * @param i
   * @param c
   * @return 
   */
  public boolean addAll(int i, Collection c) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("set(int,Object)"));
  }
  
  /**
   * This method is not implemented and throws
   * a gate.util.MethodNotImplementedException
   * 
   * @param c
   * @return
   */
  public boolean containsAll(Collection c) {
    throw new gate.util.MethodNotImplementedException(
            notImplementedMessage("containsAll(Collection)"));
  }
  
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

  @Override
  public void resourceLoaded(CreoleEvent e) {
  }

  @Override
  public void resourceRenamed(
          Resource resource,
          String oldName,
          String newName) {
    // if one of our documents gets renamed, rename it back and
    // write an error message
    if(resource instanceof Document) {
      Document doc = (Document)resource;
      if(loadedDocuments.containsValue(doc)) {
        System.err.println("ERROR: documents from a virtual corpus cannot be renamed!");
        doc.setName(oldName);
      }
    }
  }

  @Override
  public void resourceUnloaded(CreoleEvent e) {
    Resource res = e.getResource();
    if(res instanceof Document) {
      Document doc = (Document)res;
      // check if this document has actually been loaded by us
      if(loadedDocuments.containsValue(doc)) {
        unloadDocument(doc);
      } // else: its not ours, ignore
    } else if(res == this) {
      // if this corpus object gets unloaded, what should we do with any 
      // of the documents which are currently loaded?
      // TODO!!!!
      // Also should we not do the cleanup in the cleanup code?
      Gate.getCreoleRegister().removeCreoleListener(this);
    }
  }
  
  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   *
   * @param i
   * @return
   */
  @Override
  public ListIterator<Document> listIterator(int i) {
    throw new MethodNotImplementedException(
            notImplementedMessage("listIterator(int)"));
  }

  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   *
   *
   * @param i
   * @return
   */
  @Override
  public ListIterator<Document> listIterator() {
    throw new MethodNotImplementedException(
            notImplementedMessage("listIterator()"));
  }

  
  
  /**
   * This method is not implemented and always throws a
   * MethodNotImplementedException.
   * 
   * @param docObj
   * @return
   */
  @Override
  public int lastIndexOf(Object docObj) {
    throw new MethodNotImplementedException(
            notImplementedMessage("lastIndexOf(Object)"));
  }


  /**
   * Check if the corpus is empty.
   *
   * @return true if the corpus is empty
   */
  @Override
  public boolean isEmpty() {
    return (documentNames.isEmpty());
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

  @Override
  public void datastoreClosed(CreoleEvent ev) {
  }
  
  @Override
  public void datastoreCreated(CreoleEvent ev) {
    
  }
  
  @Override
  public void datastoreOpened(CreoleEvent ev) {
    
  }
  
  
  
} // abstract class VirtualCorpus
