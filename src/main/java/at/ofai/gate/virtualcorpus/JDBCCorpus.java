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
import gate.event.FeatureMapListener;
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

	public static final String FEATURE_JDBC_ID = "jdbcId";
	public static final String FEATURE_JDBC_CONTENT_COLUMN = "jdbcContentColumn";

	private static final String SELECT_IDS_SQL = "SELECT ${idColumn} FROM ${tableName}";
	private static final String SELECT_SQL = "SELECT ${valueColumn} FROM ${tableName} WHERE ${idColumn} = ?";
	private static final String INSERT_SQL = "INSERT INTO ${tableName} (${idColumn}, ${valueColumn}) VALUES (?, ?)";
	private static final String UPDATE_SQL = "UPDATE ${tableName} SET ${valueColumn} = ? WHERE ${idColumn} = ?";
	private static final String ALL_COLUMNS = "*";

	protected String jdbcDriver;
	protected String jdbcUrl = "";
	protected String jdbcUser = "";
	protected String jdbcPassword = "";
	protected String tableName;
	protected String idColumn;
	protected String contentColumns;
	protected String featureColumns;

	private Map<String, Map<String, Object>> documentFeatures = new HashMap<>();
	private Connection connection = null;
	private Map<String, PreparedStatement> selectStatements;
	private Map<String, PreparedStatement> insertStatements;
	private Map<String, PreparedStatement> updateStatements;

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

	@CreoleParameter(comment = "The document content columns (separate multiple values by comma, * for all columns)", defaultValue = "*")
	public void setContentColumns(String contentColumns) {
		this.contentColumns = contentColumns;
	}

	public String getContentColumns() {
		return contentColumns;
	}

	@CreoleParameter(comment = "The document feature columns (separate multiple values by comma)", defaultValue = "")
	public void setFeatureColumns(String featureColumns) {
		this.featureColumns = featureColumns;
	}

	public String getFeatureColumns() {
		return featureColumns;
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
		if (!hasValue(contentColumns)) {
			throw new ResourceInstantiationException("valueColumn must not be empty");
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
		List<String> tableColumns;
		try {
			tableColumns = getTableColumnNames(tableName);
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not get column names", e);
		}
		if (!tableColumns.contains(idColumn)) {
			throw new ResourceInstantiationException("id column does not exist");
		}
		List<String> contentColumns = splitUserInput(this.contentColumns);
		if (contentColumns.contains(idColumn.trim())) {
			throw new ResourceInstantiationException("contentColumns cannot contain idColumn");
		}
		List<String> featureColumns = splitUserInput(this.featureColumns);
		if (featureColumns.contains(idColumn.trim())) {
			throw new ResourceInstantiationException("featureColumns cannot contain idColumn");
		}
		if (contentColumns.contains(ALL_COLUMNS)) {
			try {
				List<String> columns = getTableColumnNames(tableName);
				columns.remove(idColumn);
				columns.removeAll(featureColumns);
				contentColumns.clear();
				contentColumns.addAll(columns);
			} catch (SQLException e) {
				throw new ResourceInstantiationException("Could not load table columns", e);
			}
		}
		if (!tableColumns.containsAll(contentColumns)) {
			throw new ResourceInstantiationException("content columns does not exist");
		}
		if (!tableColumns.containsAll(featureColumns)) {
			throw new ResourceInstantiationException("feature columns does not exist");
		}

		List<String> documentNames = new ArrayList<>();
		try {
			ResultSet rs = prepareStatement(SELECT_IDS_SQL).executeQuery();
			while (rs.next()) {
				Object id = rs.getObject(idColumn);
				if (contentColumns.size() > 1) {
					for (String column : contentColumns) {
						String documentName = id + " " + column;
						Map<String, Object> features = new HashMap<>();
						features.put(FEATURE_JDBC_ID, id);
						features.put(FEATURE_JDBC_CONTENT_COLUMN, column);
						documentFeatures.put(documentName, features);
						documentNames.add(documentName);
					}
				} else {
					Map<String, Object> features = new HashMap<>();
					features.put(FEATURE_JDBC_ID, id);
					features.put(FEATURE_JDBC_CONTENT_COLUMN, contentColumns.get(0));
					documentFeatures.put(id.toString(), features);
					documentNames.add(id.toString());
				}
			}
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not read ids", e);
		}
		initVirtualCorpus(documentNames);
		try {
			selectStatements = prepareStatements(SELECT_SQL, contentColumns);
			insertStatements = prepareStatements(INSERT_SQL, contentColumns);
			updateStatements = prepareStatements(UPDATE_SQL, contentColumns);
			if (!featureColumns.isEmpty()) {
				selectStatements.putAll(prepareStatements(SELECT_SQL, featureColumns));
				updateStatements.putAll(prepareStatements(UPDATE_SQL, featureColumns));
			}
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
	protected Document readDocument(String documentName) throws Exception {
		Map<String, Object> features = documentFeatures.get(documentName);
		Object id = features.get(FEATURE_JDBC_ID);
		String contentColumn = (String) features.get(FEATURE_JDBC_CONTENT_COLUMN);
		String content = readColumnContent(id, contentColumn);

		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content != null ? content : "");
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		Document document = (Document) Factory.createResource(DocumentImpl.class.getName(), params, null, documentName);
		document.getFeatures().putAll(features);
		document.getFeatures().put("gate.SourceURL", "created from JDBC");
		List<String> featureColumns = splitUserInput(this.featureColumns);
		if (!featureColumns.isEmpty()) {
			for (String featureColumn : featureColumns) {
				Object featureValue = readColumnContent(id, featureColumn);
				document.getFeatures().put(featureColumn, featureValue);
			}
			if (!readonly) {
				document.getFeatures().addFeatureMapListener(new FeatureMapListener() {

					@Override
					public void featureMapUpdated() {
						for (String feature : featureColumns) {
							FeatureMap featureMap = document.getFeatures();
							Object value;
							if (featureMap.containsKey(feature)) {
								value = document.getFeatures().get(feature);
							} else {
								value = null;
							}
							try {
								updateColumnContent(id, feature, value);
							} catch (SQLException e) {
								throw new GateRuntimeException(e);
							}
						}
					}
				});
			}
		}
		return document;
	}

	@Override
	protected void createDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		Object id = features.get(FEATURE_JDBC_ID);
		String contentColumn = (String) features.get(FEATURE_JDBC_CONTENT_COLUMN);

		if (existColumn(id, contentColumn)) {
			updateColumnContent(id, contentColumn, export(getExporter(mimeType), document));
		} else {
			insertColumnContent(id, contentColumn, export(getExporter(mimeType), document));
		}
	}

	@Override
	protected void updateDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		Object id = features.get(FEATURE_JDBC_ID);
		String contentColumn = (String) features.get(FEATURE_JDBC_CONTENT_COLUMN);
		updateColumnContent(id, contentColumn, export(getExporter(mimeType), document));
	}

	@Override
	protected void deleteDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		Object id = features.get(FEATURE_JDBC_ID);
		String contentColumn = (String) features.get(FEATURE_JDBC_CONTENT_COLUMN);
		updateColumnContent(id, contentColumn, null);
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

	private Map<String, PreparedStatement> prepareStatements(String query, List<String> valueColumns)
			throws SQLException {
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

	private List<String> getTableColumnNames(String tableName) throws SQLException {
		List<String> columns = new ArrayList<>();
		ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null);
		while (resultSet.next()) {
			columns.add(resultSet.getString("COLUMN_NAME"));
		}
		return columns;
	}

	private boolean existColumn(Object id, String column) throws SQLException {
		PreparedStatement statement = selectStatements.get(column);
		statement.setObject(1, id);
		ResultSet rs = statement.executeQuery();
		return rs.next();
	}

	@SuppressWarnings("unchecked")
	private <R> R readColumnContent(Object id, String column) throws SQLException {
		PreparedStatement statement = selectStatements.get(column);
		statement.setObject(1, id);
		ResultSet rs = statement.executeQuery();
		if (!rs.next()) {
			throw new GateRuntimeException("No row found in table: " + id);
		}

		Object content = rs.getObject(1);

		if (rs.next()) {
			throw new GateRuntimeException("More than one row found in table: " + id);
		}
		return (R) content;
	}

	private void updateColumnContent(Object id, String column, Object value) throws SQLException {
		PreparedStatement updateContentStatement = updateStatements.get(column);
		updateContentStatement.setObject(1, value);
		updateContentStatement.setObject(2, id);
		updateContentStatement.executeUpdate();
	}

	private void insertColumnContent(Object id, String column, String value) throws SQLException {
		PreparedStatement updateContentStatement = insertStatements.get(column);
		updateContentStatement.setObject(1, id);
		updateContentStatement.setObject(2, value);
		updateContentStatement.executeUpdate();
	}

}