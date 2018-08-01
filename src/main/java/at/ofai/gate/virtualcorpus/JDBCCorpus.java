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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.FeatureMap;
import gate.Resource;
import gate.corpora.DocumentImpl;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.util.GateRuntimeException;

/**
 * A Corpus LR that mirrors documents stored in a JDBC database table field.
 * 
 * The table must have a unique id field which will serve as the document name
 * and it must have a field that contains the actual document in some format
 * that can be both read, and if readonly is not true, also written by GATE
 * using the currently loaded plugins. The format used by default is gate XML,
 * however it is possible to specify a different format by specifying a mime
 * type when the corpus is created.
 * <p>
 * NOTE: this corpus is immutable, none of the methods to add or remove
 * documents is supported!
 */

@CreoleResource(name = "JDBCCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents stored in a JDBC table")
public class JDBCCorpus extends VirtualCorpus implements Corpus {
	private static final long serialVersionUID = -8485133333415382902L;
	private static Logger logger = Logger.getLogger(JDBCCorpus.class);

	private static final String SELECT_NAMES_SQL = "SELECT ${documentNameField} from ${tableName}";
	private static final String SELECT_CONTENT_SQL = "SELECT ${documentContentField} from ${tableName} WHERE ${documentNameField} = ?";
	private static final String INSERT_SQL = "INSERT INTO ${tableName} (${documentNameField}, ${documentContentField}) VALUES (?, ?)";
	private static final String UPDATE_NAME_SQL = "UPDATE ${tableName SET ${documentNameField} = ? WHERE ${documentNameField} = ?";
	private static final String UPDATE_CONTENT_SQL = "UPDATE ${tableName} SET ${documentContentField} = ? WHERE ${documentNameField} = ?";
	private static final String DELETE_SQL = "DELETE ${tableName} WHERE ${documentNameField} = ?";

	protected String jdbcDriver;
	protected String jdbcUrl = "";
	protected String jdbcUser = "";
	protected String jdbcPassword = "";
	protected String tableName;
	protected String documentNameField;
	protected String documentContentField;
	protected String mimeType = "";

	private Connection dbConnection = null;
	private PreparedStatement selectContentStatement = null;
	private PreparedStatement insertStatement = null;
	private PreparedStatement updateNameStatement = null;
	private PreparedStatement updateContentStatement = null;
	private PreparedStatement deleteStatement = null;

	@CreoleParameter(comment = "The JDBC driver to use", defaultValue = "org.sqlite.JDBC")
	public void setJdbcDriver(String driver) {
		jdbcDriver = driver;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	@CreoleParameter(comment = "The JDBC URL, may contain $prop{name} or $env{name} or ${relpath}", defaultValue = "jdbc:sqlite:")
	public void setJdbcUrl(String url) {
		jdbcUrl = url;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	@Optional
	@CreoleParameter(comment = "The JDBC user id", defaultValue = "")
	public void setJdbcUser(String user) {
		jdbcUser = user;
	}

	public String getJdbcUser() {
		return jdbcUser;
	}

	@Optional
	@CreoleParameter(comment = "The JDBC password", defaultValue = "")
	public void setJdbcPassword(String pw) {
		jdbcPassword = pw;
	}

	public String getJdbcPassword() {
		return jdbcPassword;
	}

	@CreoleParameter(comment = "The database table name", defaultValue = "")
	public void setTableName(String name) {
		tableName = name;
	}

	public String getTableName() {
		return tableName;
	}

	@CreoleParameter(comment = "The document id/name field name", defaultValue = "")
	public void setDocumentNameField(String name) {
		documentNameField = name;
	}

	public String getDocumentNameField() {
		return documentNameField;
	}

	@CreoleParameter(comment = "The document content field name", defaultValue = "")
	public void setDocumentContentField(String name) {
		documentContentField = name;
	}

	public String getDocumentContentField() {
		return documentContentField;
	}

	@Override
	@Optional
	@CreoleParameter(comment = "Mime type of content, if empty, GATE XML is assumed", defaultValue = "")
	public void setMimeType(String type) {
		mimeType = type;
	}

	@Override
	public String getMimeType() {
		return mimeType;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		if (getTableName() == null || getTableName().equals("")) {
			throw new ResourceInstantiationException("tableName must not be empty");
		}
		if (getDocumentNameField() == null || getDocumentNameField().equals("")) {
			throw new ResourceInstantiationException("documentNameField must not be empty");
		}
		if (getDocumentContentField() == null || getDocumentContentField().equals("")) {
			throw new ResourceInstantiationException("documentContentField must not be empty");
		}
		try {
			Class.forName(getJdbcDriver());
		} catch (ClassNotFoundException e) {
			throw new ResourceInstantiationException("could not load jdbc driver");
		}
		try {
			dbConnection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
		} catch (Exception e) {
			throw new ResourceInstantiationException("Could not get driver/connection", e);
		}
		try {
			ResultSet rs = prepareStatement(SELECT_NAMES_SQL).executeQuery();
			List<String> documentNames = new ArrayList<>();
			while (rs.next()) {
				String docName = rs.getString(getDocumentNameField());
				documentNames.add(docName);
			}
			initDocuments(documentNames);
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Problem accessing database", e);
		}
		try {
			selectContentStatement = prepareStatement(SELECT_CONTENT_SQL);
			insertStatement = prepareStatement(INSERT_SQL);
			updateNameStatement = prepareStatement(UPDATE_NAME_SQL);
			updateContentStatement = prepareStatement(UPDATE_CONTENT_SQL);
			deleteStatement = prepareStatement(DELETE_SQL);
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not prepare statement", e);
		}

		return this;
	}

	@Override
	public void cleanup() {
		try {
			if (dbConnection != null && !dbConnection.isClosed()) {
				dbConnection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected Document readDocument(String documentName) {
		try {
			selectContentStatement.setString(1, documentName);
			ResultSet rs = selectContentStatement.executeQuery();
			if (!rs.next()) {
				throw new GateRuntimeException("Document not found int the DB table: " + documentName);
			}

			String content = rs.getString(1);

			if (rs.next()) {
				throw new GateRuntimeException("More than one row found for document name " + documentName);
			}

			FeatureMap params = Factory.newFeatureMap();
			params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
			params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
			params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
			return (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, documentName);
		} catch (Exception e) {
			throw new GateRuntimeException("Exception creating the document", e);
		}
	}

	@Override
	protected void createDocument(Document document) {
		try {
			insertStatement.setString(1, document.getName());
			insertStatement.setString(2, document.getContent().toString());
			insertStatement.executeUpdate();
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	@Override
	protected void updateDocument(Document document) {
		try {
			updateContentStatement.setString(2, document.getName());
			updateContentStatement.setString(1, export(getExporter(mimeType), document, encoding));
			updateContentStatement.executeUpdate();
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	@Override
	protected void deleteDocument(Document document) {
		try {
			deleteStatement.setString(1, document.getName());
			deleteStatement.executeUpdate();
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) {
		try {
			updateNameStatement.setString(2, oldName);
			updateNameStatement.setString(1, newName);
			updateNameStatement.executeUpdate();
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	private PreparedStatement prepareStatement(String query) throws SQLException {
		query = query.replaceAll(Pattern.quote("${tableName}"), getTableName());
		query = query.replaceAll(Pattern.quote("${documentNameField}"), getDocumentNameField());
		query = query.replaceAll(Pattern.quote("${documentContentField}"), getDocumentContentField());
		return dbConnection.prepareStatement(query);
	}

}