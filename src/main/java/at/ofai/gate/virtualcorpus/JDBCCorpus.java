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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
	private static final String EXIST_ID_SQL = "SELECT 1 FROM ${tableName} WHERE ${idColumn} = ?";
	private static final String SELECT_SQL = "SELECT  ${idColumn}, ${columns} FROM ${tableName} WHERE ${idColumn} IN (? ${otherIdMarks})";
	private static final String INSERT_SQL = "INSERT INTO ${tableName} (${idColumn}, ${valueColumn}) VALUES (?, ?)";
	private static final String UPDATE_SQL = "UPDATE ${tableName} SET ${valueColumn} = ? WHERE ${idColumn} = ?";
	private static final String ALL_COLUMNS = "*";

	protected String jdbcDriver;
	protected String jdbcUrl;
	protected String jdbcUser;
	protected String jdbcPassword;
	protected String tableName;
	protected String idColumn;
	protected String contentColumns;
	protected String featureColumns;
	protected Integer preloadDocuments;

	private List<Object> allIds = new ArrayList<>();
	private List<Object> loadedIds = new ArrayList<>();
	private Map<String, Map<String, Object>> documentFeatures = new HashMap<>();
	private Map<Map<String, Object>, String> documentFeaturesReversed = new HashMap<>();
	private Map<Document, FeatureMapListener> featureUpdaters = new HashMap<>();
	private Connection connection = null;
	private List<String> columns;
	private List<String> contentColumnList;
	private List<String> featureColumnList;
	private PreparedStatement selectStatement;
	private PreparedStatement existStatement;
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

	@CreoleParameter(comment = "preload n more documents, if a document loaded (e.g. usefull for batch mode)", defaultValue = "0")
	public void setPreloadDocuments(Integer preloadDocuments) {
		this.preloadDocuments = preloadDocuments;
	}

	public Integer getPreloadDocuments() {
		return preloadDocuments;
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
		this.idColumn = this.idColumn.trim();
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
		if (contentColumns.contains(idColumn)) {
			throw new ResourceInstantiationException("contentColumns cannot contain " + idColumn);
		}
		List<String> featureColumns = splitUserInput(this.featureColumns);
		if (featureColumns.contains(idColumn)) {
			throw new ResourceInstantiationException("featureColumns cannot contain " + idColumn);
		}
		if (featureColumns.contains(FEATURE_JDBC_ID)) {
			throw new ResourceInstantiationException("featureColumns cannot contain " + FEATURE_JDBC_ID);
		}
		if (featureColumns.contains(FEATURE_JDBC_CONTENT_COLUMN)) {
			throw new ResourceInstantiationException("featureColumns cannot contain " + FEATURE_JDBC_CONTENT_COLUMN);
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
			contentColumns.removeAll(tableColumns);
			throw new ResourceInstantiationException("content columns does not exist: " + contentColumns);
		}
		if (!tableColumns.containsAll(featureColumns)) {
			featureColumns.removeAll(tableColumns);
			throw new ResourceInstantiationException("feature columns does not exist: " + featureColumns);
		}
		this.contentColumnList = contentColumns;
		this.featureColumnList = featureColumns;
		this.columns = new ArrayList<>();
		this.columns.addAll(contentColumns);
		this.columns.addAll(featureColumns);

		try {
			selectStatement = prepareStatement(SELECT_SQL);
			existStatement = prepareStatement(EXIST_ID_SQL);
			insertStatements = prepareStatements(INSERT_SQL, contentColumns);
			updateStatements = prepareStatements(UPDATE_SQL, contentColumns);
			if (!featureColumns.isEmpty()) {
				updateStatements.putAll(prepareStatements(UPDATE_SQL, featureColumns));
			}
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not prepare statement", e);
		}

		List<String> documentNames = new ArrayList<>();
		try {
			ResultSet rs = prepareStatement(SELECT_IDS_SQL).executeQuery();
			while (rs.next()) {
				Object id = rs.getObject(idColumn);
				allIds.add(id);
				if (contentColumns.size() > 1) {
					for (String column : contentColumns) {
						String documentName = id + " " + column;
						Map<String, Object> features = new HashMap<>();
						features.put(FEATURE_JDBC_ID, id);
						features.put(FEATURE_JDBC_CONTENT_COLUMN, column);
						documentFeatures.put(documentName, features);
						documentFeaturesReversed.put(features, documentName);
						documentNames.add(documentName);
					}
				} else {
					Map<String, Object> features = new HashMap<>();
					features.put(FEATURE_JDBC_ID, id);
					features.put(FEATURE_JDBC_CONTENT_COLUMN, contentColumns.get(0));
					documentFeatures.put(id.toString(), features);
					documentFeaturesReversed.put(features, id.toString());
					documentNames.add(id.toString());
				}
			}
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not read ids", e);
		}

		initVirtualCorpus(documentNames);

		return this;
	}

	@Override
	public void cleanup() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			throw new GateRuntimeException(e);
		}
	}

	@Override
	protected Document readDocument(String documentName) throws Exception {
		Map<String, Object> features = documentFeatures.get(documentName);
		Object id = features.get(FEATURE_JDBC_ID);

		List<Object> preloadIds = new ArrayList<Object>();
		for (int i = allIds.indexOf(id) + 1; i < allIds.size(); i++) {
			if ((preloadIds.size() + 1) * contentColumnList.size() >= preloadDocuments) {
				break;
			}
			if (!loadedIds.contains(id)) {
				preloadIds.add(allIds.get(i));
			}
		}

		Map<Object, Map<String, Object>> rowValues = readRows(id, preloadIds);
		Map<String, Document> readDocuments = new LinkedHashMap<String, Document>();

		for (Entry<Object, Map<String, Object>> entry : rowValues.entrySet()) {
			Object readId = entry.getKey();
			Map<String, Object> values = entry.getValue();
			for (String contentColumn : contentColumnList) {
				Map<String, Object> readFeatures = new HashMap<>();
				readFeatures.put(FEATURE_JDBC_ID, readId);
				readFeatures.put(FEATURE_JDBC_CONTENT_COLUMN, contentColumn);
				String readDocumentName = documentFeaturesReversed.get(readFeatures);

				Object content = values.get(contentColumn);

				FeatureMap params = Factory.newFeatureMap();
				params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content != null ? content : "");
				params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
				params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
				Document document = (Document) Factory.createResource(DocumentImpl.class.getName(), params, null,
						readDocumentName);
				document.getFeatures().putAll(features);
				document.getFeatures().put("gate.SourceURL", "created from JDBC");
				if (!featureColumnList.isEmpty()) {
					for (String featureColumn : featureColumnList) {
						Object featureValue = values.get(featureColumn);
						document.getFeatures().put(featureColumn, featureValue);
					}
					if (!readonly) {
						FeatureMapListener featureUpdater = new FeatureMapListener() {

							@Override
							public void featureMapUpdated() {
								for (String feature : featureColumnList) {
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
						};
						document.getFeatures().addFeatureMapListener(featureUpdater);
						featureUpdaters.put(document, featureUpdater);
					}
				}
				readDocuments.put(readDocumentName, document);
			}
			loadedIds.add(readId);
		}
		Document read = null;

		for (Entry<String, Document> entry : readDocuments.entrySet()) {
			String name = entry.getKey();
			Document document = entry.getValue();
			if (name == documentName) {
				read = document;
			} else {
				documentLoaded(name, document);
			}
		}

		return read;
	}

	@Override
	protected void documentUnloaded(Document document) {
		FeatureMap features = document.getFeatures();
		Object id = features.get(FEATURE_JDBC_ID);
		loadedIds.remove(id);
		features.removeFeatureMapListener(featureUpdaters.remove(document));
	}

	@Override
	protected void createDocument(Document document) throws Exception {
		Map<Object, Object> features = document.getFeatures();
		Object id = features.get(FEATURE_JDBC_ID);
		String contentColumn = (String) features.get(FEATURE_JDBC_CONTENT_COLUMN);

		if (existId(id)) {
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
		query = query.replaceAll(Pattern.quote("${columns}"), String.join(",", this.columns));
		query = query.replaceAll(Pattern.quote("${otherIdMarks}"),
				String.join("", Collections.nCopies(preloadDocuments, ", ?")));
		return connection.prepareStatement(query);
	}

	private Map<String, PreparedStatement> prepareStatements(String query, List<String> columns) throws SQLException {
		Map<String, PreparedStatement> statements = new HashMap<>();
		for (String column : columns) {
			String columnQuery = query.replaceAll(Pattern.quote("${valueColumn}"), column);
			PreparedStatement statement = prepareStatement(columnQuery);
			statements.put(column, statement);
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

	private boolean existId(Object id) throws SQLException {
		existStatement.setObject(1, id);
		ResultSet rs = existStatement.executeQuery();
		return rs.next();
	}

	private Map<Object, Map<String, Object>> readRows(Object loadId, List<Object> preloadIds) throws SQLException {
		selectStatement.setObject(1, loadId);
		for (int i = 0; i < preloadDocuments; i++) {
			if (i < preloadIds.size()) {
				Object otherId = preloadIds.get(i);
				selectStatement.setObject(i + 2, otherId);
			} else {
				selectStatement.setNull(i + 2, java.sql.Types.JAVA_OBJECT);
			}
		}

		ResultSet rs = selectStatement.executeQuery();
		Map<Object, Map<String, Object>> values = new LinkedHashMap<>();
		while (rs.next()) {
			Object id = rs.getObject(idColumn);

			Map<String, Object> rowValues = new LinkedHashMap<>();
			for (String column : columns) {
				rowValues.put(column, rs.getObject(column));
			}

			values.put(id, rowValues);
		}

		if (!values.keySet().contains(loadId) || !values.keySet().containsAll(preloadIds)) {
			throw new GateRuntimeException("missing values");
		}
		return values;
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