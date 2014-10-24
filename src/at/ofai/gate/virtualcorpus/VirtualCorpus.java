/*
 *  VirtualCorpus.java
 *
 * Copyright (c) 2010, Austrian Research Institute for
 * Artificial Intelligence (OFAI)
 *
 * This file is free
 * software, licenced under the GNU Library General Public License,
 * Version 2, June1991.
 *
 *  Johann Petrak, 30/8/2010
 *
 *  $Id: VirtualCorpus.java 119 2014-03-04 11:56:41Z johann.petrak $
 */

package at.ofai.gate.virtualcorpus;


import gate.Corpus;
import gate.Document;
import gate.creole.*;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.Optional;
import java.io.FileFilter;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * 
 * @author Johann Petrak
 */
public abstract class VirtualCorpus
  extends AbstractLanguageResource implements Corpus
{

  /**
   * Setter for the <code>removeDocuments</code> LR initialization parameter.
   *
   * @param removeDocuments If set to false, do not remove the stored version
   * of the document if the document is deleted from the corpus inside GATE.
   *
   */
  @Optional
  @CreoleParameter(comment="if false, do not delete original if document is removed from the corpus",
    defaultValue="true")
  public void setRemoveDocuments(Boolean removeDocuments) {
    this.removeDocuments = removeDocuments;
  }
  /**
   * Getter for the <code>saveDocuments</code> LR initialization parameter.
   *
   * @return true if documents are saved to the filesystem on unload, false
   * otherwise.
   */
  public Boolean getRemoveDocuments() {
    return this.removeDocuments;
  }
  protected Boolean removeDocuments = true;

  /**
   * Setter for the <code>saveDocuments</code> LR initialization parameter.
   *
   * @param saveDocuments If set to false, nothing will be writte to the file
   * system. If set to true (the default), then modified documents are written
   * to either the directoryURL or, if specified, the outDirectoryURL directory.
   *
   */
  @Optional
  @CreoleParameter(comment="If false, nothing is ever saved",
    defaultValue="true")
  public void setSaveDocuments(Boolean saveDocuments) {
    this.saveDocuments = saveDocuments;
  }
  /**
   * Getter for the <code>saveDocuments</code> LR initialization parameter.
   *
   * @return true if documents are saved to the filesystem on unload, false
   * otherwise.
   */
  public Boolean getSaveDocuments() {
    return this.saveDocuments;
  }
  protected Boolean saveDocuments = true;

  /**
   * @param setTransient
   */
  @CreoleParameter(comment = "If the corpus created should be transient or use a dummy datastore",
    defaultValue="true")
  public void setTransientCorpus(Boolean setTransient) {
    isTransientCorpus = setTransient;
  }
  /**
   * @return
   */
  public Boolean getTransientCorpus() {
    return isTransientCorpus;
  }

  protected Boolean isTransientCorpus = true;

  @Optional
  @CreoleParameter(comment = "Documents are expected to be compressed and stored in compressed form",
    defaultValue="false")
  public void setUseCompression(Boolean flag) {
    useCompression = flag;
  }
  public Boolean getUseCompression() {
    return useCompression;
  }
  protected Boolean useCompression = false;
  
  @Optional
  @CreoleParameter(comment = "Documents are compressed if saved to a different target",
    defaultValue="false")
  public void setCompressOnCopy(Boolean flag) {
    compressOnCopy = flag;
  }
  public Boolean getCompressOnCopy() {
    return compressOnCopy;
  }
  protected Boolean compressOnCopy = false;
  
  
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
    return "Method "+methodName+" not implemented for corpus "+
            this.getName()+" of class "+this.getClass();
  }

  boolean dataStoreIsHidden = false;

  /**
   * 
   */
  public void hideDataStore() {
    dataStoreIsHidden = true;
  }
  /**
   * 
   */
  public void unHideDataStore() {
    dataStoreIsHidden = false;
  }
  Map<String,Integer> documentIndexes = new HashMap<String,Integer>();

  public abstract boolean isDocumentLoaded(int index);
  public abstract void unloadDocument(Document doc);
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
  
  
} // abstract class VirtualCorpus
