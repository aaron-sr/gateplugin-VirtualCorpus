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

import gate.*;
import gate.creole.metadata.*;
import gate.event.CreoleListener;

/**
 * A directory corpus that contains a subset of the documents of an existing
 * DirectoryCorpus (its "parent corpus"). Only documents that are in the parent
 * corpus can be added, and removing a document from the SubsetCorpus will not
 * delete any files in the directory. This corpus is essentially just a view
 * into the parent corpus. Its main purpose is to support the Learning plugin
 * with a DirectoryCorpus.
 * <p>
 * NOTE: for now, only a non-transient DirectoryCorpus can have a
 * DirectorySubsetCorpus.
 * <p>
 * NOTE: for now, events on the parent corpus are not all handled correctly,
 * e.g. if a document gets removed from the parent, this corpus might not adapt
 * to it. For now, a subset corpus should only be used while the parent corpus
 * stays unchanged!!!!!
 * <p>
 * Removing a SubsetCorpus will not remove the datastore. The normal way to
 * create a SubsetCorpus is by adopting a new and empty transient corpus to the
 * datastore of an existing DirectoryCorpus. However, it can also be
 * instantiated directly (the only required parameter is an existing
 * DirectoryCorpus).
 * 
 * @author Johann Petrak
 */
@CreoleResource(name = "DirectorySubsetCorpus", interfaceName = "gate.Corpus", icon = "corpus", helpURL = "http://code.google.com/p/gateplugin-virtualcorpus/wiki/DirectoryCorpusUsage", comment = "A corpus that provides a view of a subset of the documents in an existing DirectoryCorpus")
public class DirectorySubsetCorpus extends VirtualSubsetCorpus implements Corpus, CreoleListener {

	// *****
	// Fields
	// ******

	/**
	 * 
	 */
	private static final long serialVersionUID = -8485199876515382902L;

	// ***************
	// Parameters
	// ***************

	/**
	 * @param corpus
	 */
	@CreoleParameter(comment = "The DirectoryCorpus for which to create this corpus", defaultValue = "")
	public void setDirectoryCorpus(DirectoryCorpus corpus) {
		this.virtualCorpus = corpus;
	}

	/**
	 * @return
	 */
	public DirectoryCorpus getDirectoryCorpus() {
		return (DirectoryCorpus) this.virtualCorpus;
	}

} // class DirectorySubsetCorpus
