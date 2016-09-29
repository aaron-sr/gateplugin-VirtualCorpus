/*
 * Copyright (c) 2010- Austrian Research Institute for Artificial Intelligence (OFAI). 
 * Copyright (C) 2013-2016 The University of Sheffield.
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
import gate.Document;
import gate.LanguageResource;
import gate.corpora.CorpusImpl;
import gate.creole.ResourceInstantiationException;
import gate.persist.PersistenceException;
import gate.Factory;
import gate.FeatureMap;
import gate.Resource;


/**
 * A dummy datastore so that documents that are returned by a DirectoryCorpus
 * are not looking like transient documents. This will help getting them
 * unloaded.
 * 
 * @author Johann Petrak
 */
public class DummyDataStore4DirCorp
  extends DummyDataStore4Virtuals {


  /**
   *
   * @param lr
   * @throws PersistenceException
   */
  @Override
  public void sync(LanguageResource lr) throws PersistenceException {
    if(lr instanceof Document && ourCorpus.isDocumentLoaded((Document)lr)) {
      //System.err.println("Syncing document: "+lr.getName());
      ourCorpus.saveDocument((Document)lr);
    } else {
      //System.err.println("Ignoring sync for: "+lr.getName());
    }
  }

  /**
   * Adopting works a bit different for this dummy data store. The directory
   * corpus allows to directly add a document to the corpus. All documents
   * that are added to the corpus will get their datastore set to this data
   * store. If they already have a different data store set, an exception
   * is thrown.
   * If adopt is called via e.g. doc.getDataStore.adopt(doc) then it is
   * noop, because a doc where getDataStore returns this datastore has already
   * been added to the directory corpus.
   * If adopt is called via doc.getDataStore.adopt(someotherlr) an exception
   * is thrown since the datastore cannot adopt any other LRs.
   * 
   * @param lr
   * @param secInfo
   * @return
   * @throws PersistenceException
   */
  @Override
  public LanguageResource adopt(LanguageResource langres) throws PersistenceException {
    LanguageResource lr = langres;
    if(lr instanceof Document) {
      Document doc = (Document)lr;
      if(doc.getDataStore() == null || doc.getDataStore() != this) {
        throw new PersistenceException("Cannot adopt document, already in a different datastore: "+lr.getName());
      }
      // otherwise, the document is already adopted by this datastore so we
      // silently ignore this.
    } else if(lr instanceof CorpusImpl) {
      // only a transient, empty corpus can be adopted!!!
      Corpus corpus = (Corpus)lr;
      if(corpus.getDataStore() != null) {
        throw new PersistenceException(
          "Cannot adopt corpus "+corpus.getName()+
          " which belongs to datastore "+corpus.getDataStore().getName());
      }
      if(corpus.size() != 0) {
        throw new PersistenceException(
          "Cannot adopt corpus "+corpus.getName()+
          " which is non empty, number of documents contained: "+
          corpus.size());
      }
      // since this is a valid corpus, we adopt it by returning new
      // DocumentSubsetCorpus which has the original corpus as a parent
      FeatureMap parms = Factory.newFeatureMap();
      parms.put("directoryCorpus", ourCorpus);
      try {
        Resource newCorpus = Factory.createResource("at.ofai.gate.virtualcorpus.DirectorySubsetCorpus", parms, corpus.getFeatures(), corpus.getName());
        lr = (LanguageResource)newCorpus;
      } catch (ResourceInstantiationException ex) {
        throw new PersistenceException("Could not adopt corpus "+corpus.getName(),ex);
      }
    } else {
      throw new PersistenceException("Cannot adopt LR: "+lr.getName());
    }
    return lr;
  }

  @Override
  public String toString() {
    return "DummyDataStore4DirCorp "+this.getName()+" for "+getCorpusName();
  }

  @Override
  public void close() {
    System.err.println("This resource cannot be closed, it will be closed automatically when DirectoryCorpus "+getCorpusName()+" is closed");
  }


  private String getCorpusName() {
    if(getCorpus() != null) {
      return getCorpus().getName();
    } else {
      return "";
    }
  }

}
