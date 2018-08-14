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
import java.util.Properties;
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

	private static final String SELECT_ID_SQL = "SELECT ${idColumn} FROM ${tableName}";
	private static final String SELECT_CONTENT_SQL = "SELECT ${valueColumn} FROM ${tableName} WHERE ${idColumn} = ?";
	private static final String INSERT_SQL = "INSERT INTO ${tableName} (${idColumn}, ${valueColumn}) VALUES (?, ?)";
	private static final String UPDATE_CONTENT_SQL = "UPDATE ${tableName} SET ${valueColumn} = ? WHERE ${idColumn} = ?";
	private static final String ALL_COLUMNS = "*";

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
	private Map<String, PreparedStatement> updateContentStatements;

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

	@CreoleParameter(comment = "The document content column (separate multiple values by comma, * for all columns)", defaultValue = "")
	public void setValueColumn(String valueColumn) {
		this.valueColumn = valueColumn;
	}

	public String getValueColumn() {
		return valueColumn;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType();
		if (!hasValue(tableName)) {
			throw new ResourceInstantiationException("tableName must not be empty");
		}
		if (!hasValue(idColumn)) {
			throw new ResourceInstantiationException("idColumn must not be empty");
		}
		if (!hasValue(valueColumn)) {
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
			throw new ResourceInstantiationException("could not load jdbc driver", e);
		}
		try {
			Properties properties = new Properties();
			if (jdbcUser != null) {
				properties.put("user", jdbcUser);
			}
			if (jdbcPassword != null) {
				properties.put("password", jdbcPassword);
			}
			if (encoding != null) {
				properties.put("characterEncoding", encoding);
			}
			connection = DriverManager.getConnection(jdbcUrl, properties);
		} catch (Exception e) {
			throw new ResourceInstantiationException("Could not get driver/connection", e);
		}
		if (valueColumns.contains(ALL_COLUMNS)) {
			try {
				List<String> columns = getColumns(tableName);
				columns.remove(idColumn);
				valueColumns.clear();
				valueColumns.addAll(columns);
			} catch (SQLException e) {
				throw new ResourceInstantiationException("could not load table columns", e);
			}
		}

		List<String> documentNames = new ArrayList<>();
		try {
			ResultSet rs = prepareStatement(SELECT_ID_SQL).executeQuery();
			while (rs.next()) {
				String id = rs.getString(idColumn);
				if (valueColumns.size() > 1) {
					for (String column : valueColumns) {
						String documentName = id + " " + column;
						Map<String, String> features = new HashMap<>();
						features.put(JDBC_ID, id);
						features.put(JDBC_CONTENT_COLUMN, column);
						documentFeatures.put(documentName, features);
						documentNames.add(documentName);
					}
				} else {
					Map<String, String> features = new HashMap<>();
					features.put(JDBC_ID, id);
					features.put(JDBC_CONTENT_COLUMN, valueColumns.get(0));
					documentFeatures.put(id, features);
					documentNames.add(id);
				}
			}
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Problem accessing database", e);
		}
		initVirtualCorpus(documentNames);
		try {
			selectContentStatements = prepareStatements(SELECT_CONTENT_SQL);
			insertStatements = prepareStatements(INSERT_SQL);
			updateContentStatements = prepareStatements(UPDATE_CONTENT_SQL);
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not prepare statement", e);
		}

		return this;
	}

	private List<String> getColumns(String tableName) throws SQLException {
		List<String> columns = new ArrayList<>();
		ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null);
		while (resultSet.next()) {
			columns.add(resultSet.getString("COLUMN_NAME"));
		}

		return columns;
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
	protected Document readDocument(String documentName) throws Exception {
		Map<String, String> features = documentFeatures.get(documentName);
		String id = features.get(JDBC_ID);
		String contentColumn = features.get(JDBC_CONTENT_COLUMN);
		PreparedStatement selectContentStatement = selectContentStatements.get(contentColumn);
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
		params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content != null ? content : "");
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		Document document = (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, documentName);
		document.getFeatures().putAll(features);
		return document;
	}

	@Override
	protected void createDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		String id = features.get(JDBC_ID).toString();
		String contentColumn = features.get(JDBC_CONTENT_COLUMN).toString();

		PreparedStatement selectContentStatement = selectContentStatements.get(contentColumn);
		selectContentStatement.setString(1, id);
		ResultSet rs = selectContentStatement.executeQuery();
		if (rs.next()) {
			PreparedStatement updateContentStatement = updateContentStatements.get(contentColumn);
			updateContentStatement.setString(2, id);
			updateContentStatement.setString(1, export(getExporter(mimeType), document));
			updateContentStatement.executeUpdate();
		} else {
			PreparedStatement insertStatement = insertStatements.get(contentColumn);
			insertStatement.setString(1, id);
			insertStatement.setString(2, export(getExporter(mimeType), document));
			insertStatement.executeUpdate();
		}
	}

	@Override
	protected void updateDocument(Document document) throws Exception {
		String documentName = document.getName();
		Map<String, String> features = documentFeatures.get(documentName);
		String id = features.get(JDBC_ID);
		String contentColumn = features.get(JDBC_CONTENT_COLUMN);
		PreparedStatement updateContentStatement = updateContentStatements.get(contentColumn);
		updateContentStatement.setString(2, id);
		updateContentStatement.setString(1, export(getExporter(mimeType), document));
		updateContentStatement.executeUpdate();
	}

	@Override
	protected void deleteDocument(Document document) throws Exception {
		String documentName = document.getName();
		Map<String, String> features = documentFeatures.get(documentName);
		String id = features.get(JDBC_ID);
		String contentColumn = features.get(JDBC_CONTENT_COLUMN);
		PreparedStatement updateContentStatement = updateContentStatements.get(contentColumn);
		updateContentStatement.setString(2, id);
		updateContentStatement.setString(1, null);
		updateContentStatement.executeUpdate();
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
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
			String columnQuery = query.replaceAll(Pattern.quote("${tableName}"), tableName);
			columnQuery = columnQuery.replaceAll(Pattern.quote("${idColumn}"), idColumn);
			columnQuery = columnQuery.replaceAll(Pattern.quote("${valueColumn}"), contentColumn);
			PreparedStatement statement = connection.prepareStatement(columnQuery);
			statements.put(contentColumn, statement);
		}
		return statements;
	}

}