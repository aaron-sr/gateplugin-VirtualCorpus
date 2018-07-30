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
/**
 * This package implements the VirtualCorpus plugin which
 * provides new corpus LRs that represent directories or JDBC tables as a
 * corpus.
 *
 * The purpose of this plugin is to make it very simple to just run existing
 * corpus controllers on the documents in a directory or on documents stored
 * in a JDBC table. Thus, the documents do not have to first get imported
 * into a serial corpus in a datastore, than get exported back.
 * Since the intended use of the directory-backed and table-backed corpora
 * is simply to access, process and write back documents, the implementations
 * do not support adding or removing documents at this time: all VirtualCorpus
 * LRs are currently immutable (i.e. the content of corpus itself cannot be
 * changed, but the documents can of course be changed). 
 */
package at.ofai.gate.virtualcorpus;
