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

import java.io.Serializable;

import gate.persist.PersistenceException;
import gate.util.persistence.LRPersistence;

/**
 * Persistence for the JDBCCorpus LR. The standard Corpus Persistence won't do
 * as it either expects a persistent corpus in which case it must have a
 * Datastore, or a transient corpus in which case all the documents are
 * serialized too. We do not want either and just serialize the initialization
 * parameters so the LR will be recreated in an identical way when loaded.
 * 
 * @author Johann Petrak
 */
public class VirtualCorpusPersistence extends LRPersistence {
	public static final long serialVersionUID = 2L;

	protected Serializable featureMap;

	/**
	 * CorpusPersistence is not suitable since it stores the complete docList
	 * 
	 * VirtualCorpus just need initParams, which are stored by ResourcePersistence
	 */
	@Override
	public void extractDataFromSource(Object source) throws PersistenceException {
		if (!(source instanceof VirtualCorpus)) {
			throw new UnsupportedOperationException(
					getClass().getName() + " can only be used for " + VirtualCorpus.class.getName() + " objects!\n"
							+ source.getClass().getName() + " is not a " + VirtualCorpus.class.getName());
		}

		super.extractDataFromSource(source);
	}

}
