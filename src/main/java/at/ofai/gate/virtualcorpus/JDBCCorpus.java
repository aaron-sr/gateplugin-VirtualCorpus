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

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import gate.event.CorpusListener;
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

	protected List<CorpusListener> listeners = new ArrayList<CorpusListener>();

	protected String jdbcDriver;

	@CreoleParameter(comment = "The JDBC driver to use", defaultValue = "org.sqlite.JDBC")
	public void setJdbcDriver(String driver) {
		jdbcDriver = driver;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	protected String jdbcUrl = "";

	@CreoleParameter(comment = "The JDBC URL, may contain $prop{name} or $env{name} or ${relpath}", defaultValue = "jdbc:sqlite:")
	public void setJdbcUrl(String url) {
		jdbcUrl = url;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	protected String jdbcUser = "";

	@Optional
	@CreoleParameter(comment = "The JDBC user id", defaultValue = "")
	public void setJdbcUser(String user) {
		jdbcUser = user;
	}

	public String getJdbcUser() {
		return jdbcUser;
	}

	protected String jdbcPassword = "";

	@Optional
	@CreoleParameter(comment = "The JDBC password", defaultValue = "")
	public void setJdbcPassword(String pw) {
		jdbcPassword = pw;
	}

	public String getJdbcPassword() {
		return jdbcPassword;
	}

	protected URL dbDirectoryUrl = null;

	@Optional
	@CreoleParameter(comment = "The location of where a file database is stored. This is not used directly but can be used to replace the ${dbdirectory} variable in the jdbcUrl parameter", defaultValue = "file://.")
	public void setDbDirectoryUrl(URL dir) {
		dbDirectoryUrl = dir;
	}

	public URL getDbDirectoryUrl() {
		return dbDirectoryUrl;
	}

	/**
	 */
	@CreoleParameter(comment = "The database table name", defaultValue = "")
	public void setTableName(String name) {
		tableName = name;
	}

	public String getTableName() {
		return tableName;
	}

	protected String tableName;

	@CreoleParameter(comment = "The document id/name field name", defaultValue = "")
	public void setDocumentNameField(String name) {
		documentNameField = name;
	}

	public String getDocumentNameField() {
		return documentNameField;
	}

	protected String documentNameField;

	@CreoleParameter(comment = "The document content field name", defaultValue = "")
	public void setDocumentContentField(String name) {
		documentContentField = name;
	}

	public String getDocumentContentField() {
		return documentContentField;
	}

	protected String documentContentField;

	@Optional
	@CreoleParameter(comment = "Mime type of content, if empty, GATE XML is assumed", defaultValue = "")
	public void setMimeType(String type) {
		mimeType = type;
	}

	public String getMimeType() {
		return mimeType;
	}

	protected String mimeType = "";

	@CreoleParameter(comment = "SQL Query for selecting the set of document ids/names", defaultValue = "SELECT ${documentNameField} from ${tableName}")
	@Optional
	public void setSelectSQL(String sql) {
		this.selectSQL = sql;
	}

	/**
	 * @return
	 */
	public String getSelectSQL() {
		return this.selectSQL;
	}

	protected String selectSQL = "SELECT ${documentNameField} from ${tableName}";

	protected Connection dbConnection = null;
	protected PreparedStatement getContentStatement = null;
	protected PreparedStatement updateContentStatement = null;

	String encoding = "utf-8";

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
		if (getSelectSQL() == null || getSelectSQL().equals("")) {
			throw new ResourceInstantiationException("selectSQL must not be empty");
		}
		String query = getSelectSQL();
		query = query.replaceAll(Pattern.quote("${tableName}"), getTableName());
		query = query.replaceAll(Pattern.quote("${documentNameField}"), getDocumentNameField());
		String expandedUrl = "";
		try {
			Class.forName(getJdbcDriver());
			String dbdirectory = "";
			if (getDbDirectoryUrl().getProtocol().equals("file")) {
				dbdirectory = getDbDirectoryUrl().getPath();
				dbdirectory = new File(dbdirectory).getAbsolutePath();
			} else {
				throw new GateRuntimeException("The database directory URL is not a file URL");
			}
			Map<String, String> dbdirectoryMap = new HashMap<String, String>();
			dbdirectoryMap.put("dbdirectory", dbdirectory);

			expandedUrl = gate.Utils.replaceVariablesInString(jdbcUrl, dbdirectoryMap, this);
			String expandedUser = gate.Utils.replaceVariablesInString(jdbcUser, dbdirectoryMap, this);
			String expandedPassword = gate.Utils.replaceVariablesInString(jdbcPassword, dbdirectoryMap, this);

			System.out.println("Using JDBC URL: " + expandedUrl);
			dbConnection = DriverManager.getConnection(expandedUrl, expandedUser, expandedPassword);
		} catch (Exception ex) {
			throw new ResourceInstantiationException("Could not get driver/connection", ex);
		}
		Statement stmt = null;
		try {
			stmt = dbConnection.createStatement();
			ResultSet rs = null;
			rs = stmt.executeQuery(query);
			List<String> documentNames = new ArrayList<>();
			while (rs.next()) {
				String docName = rs.getString(getDocumentNameField());
				documentNames.add(docName);
			}
			initDocuments(documentNames);
		} catch (SQLException ex) {
			throw new ResourceInstantiationException("Problem accessing database", ex);
		}
		// create all the prepared statements we need for accessing stuff in the db
		try {
			query = "SELECT " + getDocumentContentField() + " FROM " + getTableName() + " WHERE "
					+ getDocumentNameField() + " = ?";
			System.out.println("Preparing get document statement: " + query);
			getContentStatement = dbConnection.prepareStatement(query);
		} catch (SQLException ex) {
			throw new ResourceInstantiationException("Could not prepare statement", ex);
		}

		try {
			String updstmt = "UPDATE " + getTableName() + " SET " + getDocumentContentField() + " = ? " + " WHERE "
					+ getDocumentNameField() + " = ?";
			System.out.println("Preparing update document statement: " + updstmt);
			updateContentStatement = dbConnection.prepareStatement(updstmt);
		} catch (SQLException ex) {
			throw new ResourceInstantiationException("Could not prepare statement", ex);
		}

		return this;
	}

	@Override
	public void cleanup() {
		try {
			if (dbConnection != null && !dbConnection.isClosed()) {
				dbConnection.close();
			}
		} catch (SQLException ex) {
		}
	}

	protected void saveDocument(Document doc) {
		if (getReadonly()) {
			return;
		}
		String docContent = doc.toXml();
		String docName = doc.getName();
		try {
			updateContentStatement.setString(2, docName);
			updateContentStatement.setString(1, docContent);
			updateContentStatement.execute();
		} catch (Exception ex) {
			throw new GateRuntimeException("Error when trying to update database row for document doc.getName()", ex);
		}
	}

	@Override
	protected Document readDocument(String docName) {
		try {

			ResultSet rs = null;
			String docEncoding = encoding;

			getContentStatement.setString(1, docName);
			rs = getContentStatement.executeQuery();
			if (!rs.next()) {
				throw new GateRuntimeException("Document not found int the DB table: " + docName);
			}

			String content = rs.getString(1);

			if (rs.next()) {
				throw new GateRuntimeException("More than one row found for document name " + docName);
			}

			String docMimeType = mimeType;
			FeatureMap params = Factory.newFeatureMap();
			params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
			params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, docEncoding);
			params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, docMimeType);
			return (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, docName);
		} catch (Exception ex) {
			throw new GateRuntimeException("Exception creating the document", ex);
		}
	}

}