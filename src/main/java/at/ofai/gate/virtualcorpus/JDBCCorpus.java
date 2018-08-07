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

	public static final String JDBC_ID = "jdbcId";
	public static final String JDBC_CONTENT_COLUMN = "jdbcContentColumn";

	private static final String SELECT_ID_SQL = "SELECT ${idColumn} from ${tableName}";
	private static final String SELECT_CONTENT_SQL = "SELECT ${valueColumn} from ${tableName} WHERE ${idColumn} = ?";
	private static final String INSERT_SQL = "INSERT INTO ${tableName} (${idColumn}, ${valueColumn}) VALUES (?, ?)";
	private static final String UPDATE_NAME_SQL = "UPDATE ${tableName} SET ${idColumn} = ? WHERE ${idColumn} = ?";
	private static final String UPDATE_CONTENT_SQL = "UPDATE ${tableName} SET ${valueColumn} = ? WHERE ${idColumn} = ?";
	private static final String DELETE_SQL = "DELETE ${tableName} WHERE ${idColumn} = ?";

	protected String jdbcDriver;
	protected String jdbcUrl = "";
	protected String jdbcUser = "";
	protected String jdbcPassword = "";
	protected String tableName;
	protected String idColumn;
	protected String valueColumn;
	protected String mimeType = "";

	private List<String> valueColumns = new ArrayList<>();
	private Map<String, Map<String, String>> documentFeatures = new HashMap<>();
	private Connection connection = null;
	private Map<String, PreparedStatement> selectContentStatements;
	private Map<String, PreparedStatement> insertStatements;
	private Map<String, PreparedStatement> updateNameStatements;
	private Map<String, PreparedStatement> updateContentStatements;
	private Map<String, PreparedStatement> deleteStatements;

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

	@CreoleParameter(comment = "The document id column", defaultValue = "")
	public void setIdColumn(String idColumn) {
		this.idColumn = idColumn;
	}

	public String getIdColumn() {
		return idColumn;
	}

	@CreoleParameter(comment = "The document content column (separate multiple values by comma)", defaultValue = "")
	public void setValueColumn(String valueColumn) {
		this.valueColumn = valueColumn;
	}

	public String getValueColumn() {
		return valueColumn;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		if (tableName == null || tableName.equals("")) {
			throw new ResourceInstantiationException("tableName must not be empty");
		}
		if (idColumn == null || idColumn.equals("")) {
			throw new ResourceInstantiationException("idColumn must not be empty");
		}
		if (valueColumn == null || valueColumn.equals("")) {
			throw new ResourceInstantiationException("valueColumn must not be empty");
		}
		for (String column : valueColumn.split(",")) {
			valueColumns.add(column.trim());
		}
		if (valueColumns.contains(idColumn.trim())) {
			throw new ResourceInstantiationException("valueColumn cannot be idColumn");
		}
		try {
			Class.forName(getJdbcDriver());
		} catch (ClassNotFoundException e) {
			throw new ResourceInstantiationException("could not load jdbc driver");
		}
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
		} catch (Exception e) {
			throw new ResourceInstantiationException("Could not get driver/connection", e);
		}
		List<String> documentNames = new ArrayList<>();
		try {
			ResultSet rs = prepareStatement(SELECT_ID_SQL).executeQuery();
			while (rs.next()) {
				String id = rs.getString(idColumn);
				if (valueColumns.size() > 1) {
					for (String column : valueColumns) {
						String contentName = id + " " + column;
						Map<String, String> features = new HashMap<>();
						features.put(JDBC_ID, id);
						features.put(JDBC_CONTENT_COLUMN, column);
						documentFeatures.put(contentName, features);
						documentNames.add(contentName);
					}
				} else {
					Map<String, String> features = new HashMap<>();
					features.put(JDBC_ID, id);
					features.put(JDBC_CONTENT_COLUMN, valueColumn);
					documentFeatures.put(id, features);
					documentNames.add(id);
				}
			}
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Problem accessing database", e);
		}
		initDocuments(documentNames);
		try {
			selectContentStatements = prepareStatements(SELECT_CONTENT_SQL);
			insertStatements = prepareStatements(INSERT_SQL);
			updateNameStatements = prepareStatements(UPDATE_NAME_SQL);
			updateContentStatements = prepareStatements(UPDATE_CONTENT_SQL);
			deleteStatements = prepareStatements(DELETE_SQL);
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not prepare statement", e);
		}

		return this;
	}

	@Override
	public void cleanup() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected Document readDocument(String documentName) {
		Map<String, String> features = documentFeatures.get(documentName);
		String id = features.get(JDBC_ID);
		String contentColumn = features.get(JDBC_CONTENT_COLUMN);
		PreparedStatement selectContentStatement = selectContentStatements.get(contentColumn);
		try {
			selectContentStatement.setString(1, id);
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
			FeatureMap gateFeatures = Factory.newFeatureMap();
			gateFeatures.putAll(features);
			return (Document) Factory.createResource(DocumentImpl.class.getName(), params, gateFeatures, documentName);
		} catch (Exception e) {
			throw new GateRuntimeException("Exception creating the document", e);
		}
	}

	@Override
	protected void createDocument(Document document) {
		Map<Object, Object> features = document.getFeatures();
		String id = features.get(JDBC_ID).toString();
		String contentColumn = features.get(JDBC_CONTENT_COLUMN).toString();

		try {
			PreparedStatement selectContentStatement = selectContentStatements.get(contentColumn);
			selectContentStatement.setString(1, id);
			ResultSet rs = selectContentStatement.executeQuery();
			if (rs.next()) {
				PreparedStatement updateContentStatement = updateContentStatements.get(contentColumn);
				updateContentStatement.setString(2, id);
				updateContentStatement.setString(1, export(getExporter(mimeType), document, encoding));
				updateContentStatement.executeUpdate();
			} else {
				PreparedStatement insertStatement = insertStatements.get(contentColumn);
				insertStatement.setString(1, id);
				insertStatement.setString(2, export(getExporter(mimeType), document, encoding));
				insertStatement.executeUpdate();
			}
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	@Override
	protected void updateDocument(Document document) {
		String documentName = document.getName();
		Map<String, String> features = documentFeatures.get(documentName);
		String id = features.get(JDBC_ID);
		String contentColumn = features.get(JDBC_CONTENT_COLUMN);
		PreparedStatement updateContentStatement = updateContentStatements.get(contentColumn);
		try {
			updateContentStatement.setString(2, id);
			updateContentStatement.setString(1, export(getExporter(mimeType), document, encoding));
			updateContentStatement.executeUpdate();
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	@Override
	protected void deleteDocument(Document document) {
		String documentName = document.getName();
		Map<String, String> features = documentFeatures.get(documentName);
		String id = features.get(JDBC_ID);
		String contentColumn = features.get(JDBC_CONTENT_COLUMN);
		PreparedStatement deleteStatement = deleteStatements.get(contentColumn);
		try {
			deleteStatement.setString(1, id);
			deleteStatement.executeUpdate();
		} catch (Exception e) {
			throw new GateRuntimeException("Exception inserting the document", e);
		}
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) {
		throw new GateRuntimeException("renaming document is not supported");
	}

	private PreparedStatement prepareStatement(String query) throws SQLException {
		query = query.replaceAll(Pattern.quote("${tableName}"), tableName);
		query = query.replaceAll(Pattern.quote("${idColumn}"), idColumn);
		return connection.prepareStatement(query);
	}

	private Map<String, PreparedStatement> prepareStatements(String query) throws SQLException {
		Map<String, PreparedStatement> statements = new HashMap<>();
		for (String contentColumn : valueColumns) {
			query = query.replaceAll(Pattern.quote("${tableName}"), tableName);
			query = query.replaceAll(Pattern.quote("${idColumn}"), idColumn);
			query = query.replaceAll(Pattern.quote("${valueColumn}"), contentColumn);
			PreparedStatement statement = connection.prepareStatement(query);
			statements.put(contentColumn, statement);
		}
		return statements;
	}

}