package gate.virtualcorpus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

@CreoleResource(name = "JdbcCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents stored in a JDBC table")
public class JdbcCorpus extends VirtualCorpus implements Corpus {
	private static final long serialVersionUID = -8485133333415382902L;
	private static Logger logger = Logger.getLogger(JdbcCorpus.class);

	private static final String COUNT_ID_SQL = "SELECT COUNT(${idColumn}) FROM ${tableName}";
	private static final String SELECT_ID_SQL = "SELECT ${idColumn} FROM ${tableName} ORDER BY ${idColumn} ASC";
	private static final String SELECT_VALUES_SQL = "SELECT ${idColumn}, ${columns} FROM ${tableName} ORDER BY ${idColumn} ASC";
	private static final String UPDATE_VALUES_SQL = "UPDATE ${tableName} SET ${column} = ? WHERE ${idColumn} = ?";

	private static final String ALL_COLUMNS = "*";

	protected String jdbcDriver;
	protected String jdbcUrl;
	protected String jdbcUser;
	protected String jdbcPassword;
	protected String tableName;
	protected String idColumn;
	protected String contentColumns;
	protected String featureColumns;
	protected String exportColumnSuffix;
	protected String exportEncoding;
	protected Integer resultSetType;
	protected Integer resultSetConcurrency;
	protected Integer fetchDirection;
	protected Integer fetchIds;
	protected Integer fetchRows;

	private Connection connection;
	private Collection<String> allTableColumns;
	private List<String> columns;
	private List<String> contentColumnList;
	private List<String> featureColumnList;
	private Map<String, String> exportColumnMapping;

	private PreparedStatement idStatement;
	private ResultSet idResultSet;
	private PreparedStatement valuesStatement;
	private ResultSet valuesResultSet;
	private Map<String, PreparedStatement> updateStatements;

	private Map<Integer, Object> loadedIds = new HashMap<>();

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

	@Optional
	@CreoleParameter(comment = "suffix for value columns, where exported data is written (non-existent columns will be created)", defaultValue = "")
	public void setExportColumnSuffix(String exportColumnSuffix) {
		this.exportColumnSuffix = exportColumnSuffix;
	}

	public String getExportColumnSuffix() {
		return exportColumnSuffix;
	}

	@Optional
	@CreoleParameter(comment = "encoding for value columns, which were exported (in case of reopen document)", defaultValue = "")
	public void setExportEncoding(String exportEncoding) {
		this.exportEncoding = exportEncoding;
	}

	public String getExportEncoding() {
		return exportEncoding;
	}

	@CreoleParameter(comment = "The type for the result sets (see java.sql.ResultSet TYPE_FORWARD_ONLY,TYPE_SCROLL_SENSITIVE,TYPE_SCROLL_INSENSITIVE)", defaultValue = ""
			+ ResultSet.TYPE_FORWARD_ONLY)
	public void setResultSetType(Integer resultSetType) {
		this.resultSetType = resultSetType;
	}

	public Integer getResultSetType() {
		return resultSetType;
	}

	@CreoleParameter(comment = "The concurrency for the result sets (see java.sql.ResultSet CONCUR_READ_ONLY,CONCUR_UPDATABLE)", defaultValue = ""
			+ ResultSet.CONCUR_READ_ONLY)
	public void setResultSetConcurrency(Integer resultSetConcurrency) {
		this.resultSetConcurrency = resultSetConcurrency;
	}

	public Integer getResultSetConcurrency() {
		return resultSetConcurrency;
	}

	@Optional
	@CreoleParameter(comment = "The fetch direction for the result sets (see java.sql.ResultSet FETCH_FORWARD,FETCH_REVERSE,FETCH_UNKNOWN)", defaultValue = ""
			+ ResultSet.FETCH_FORWARD)
	public void setFetchDirection(Integer fetchDirection) {
		this.fetchDirection = fetchDirection;
	}

	public Integer getFetchDirection() {
		return fetchDirection;
	}

	@Optional
	@CreoleParameter(comment = "The fetch size for the id result set", defaultValue = "0")
	public void setFetchIds(Integer fetchIds) {
		this.fetchIds = fetchIds;
	}

	public Integer getFetchIds() {
		return fetchIds;
	}

	@Optional
	@CreoleParameter(comment = "The fetch size for the documents result set", defaultValue = "0")
	public void setFetchRows(Integer fetchRows) {
		this.fetchRows = fetchRows;
	}

	public Integer getFetchRows() {
		return fetchRows;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType();
		checkValidExporterClassName();
		if (!immutableCorpus) {
			throw new ResourceInstantiationException("mutable jdbc corpus currently not supported");
		}
		if (!hasValue(tableName)) {
			throw new ResourceInstantiationException("tableName must not be empty");
		}
		if (!hasValue(idColumn)) {
			throw new ResourceInstantiationException("idColumn must not be empty");
		}
		if (!hasValue(contentColumns)) {
			throw new ResourceInstantiationException("contentColumns must not be empty");
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
		try {
			this.allTableColumns = new HashSet<>(getTableColumnNames(tableName));
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not get column names", e);
		}
		if (!allTableColumns.contains(idColumn)) {
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
		Map<String, String> exportColumnMapping = new HashMap<>();
		if (contentColumns.contains(ALL_COLUMNS)) {
			List<String> columns = new ArrayList<String>(allTableColumns);
			columns.remove(idColumn);
			columns.removeAll(featureColumns);
			if (hasValue(exportColumnSuffix)) {
				Iterator<String> iterator = columns.iterator();
				while (iterator.hasNext()) {
					String column = iterator.next();
					String columnWithoutSuffix = column.substring(0, column.length() - exportColumnSuffix.length());
					if (column.endsWith(exportColumnSuffix) && columns.contains(columnWithoutSuffix)) {
						exportColumnMapping.put(columnWithoutSuffix, column);
						iterator.remove();
					}
				}
			}
			contentColumns.clear();
			contentColumns.addAll(columns);
		} else {
			if (hasValue(exportColumnSuffix)) {
				for (String contentColumn : contentColumns) {
					String exportColum = contentColumn + exportColumnSuffix;
					exportColumnMapping.put(contentColumn, exportColum);
				}
			}
		}
		if (!allTableColumns.containsAll(contentColumns)) {
			contentColumns.removeAll(allTableColumns);
			throw new ResourceInstantiationException("content columns does not exist: " + contentColumns);
		}
		if (!allTableColumns.containsAll(featureColumns)) {
			featureColumns.removeAll(allTableColumns);
			throw new ResourceInstantiationException("feature columns does not exist: " + featureColumns);
		}
		if (!readonlyDocuments && hasValue(exportColumnSuffix)
				&& !allTableColumns.containsAll(exportColumnMapping.values())) {
			List<String> exportColumns = new ArrayList<String>(exportColumnMapping.values());
			exportColumns.removeAll(allTableColumns);
			throw new ResourceInstantiationException("export columns does not exist: " + exportColumns);
		}
		this.contentColumnList = contentColumns;
		this.featureColumnList = featureColumns;
		this.exportColumnMapping = exportColumnMapping;
		this.columns = new ArrayList<>();
		this.columns.addAll(contentColumns);
		this.columns.addAll(featureColumns);
		this.columns.addAll(exportColumnMapping.values());

		try {
			if (connection.getMetaData().supportsResultSetType(resultSetType)) {
				throw new ResourceInstantiationException("resultSetType is not supported: " + resultSetType);
			}
			if (connection.getMetaData().supportsResultSetConcurrency(resultSetType, resultSetConcurrency)) {
				throw new ResourceInstantiationException(
						"resultSetConcurrency is not supported: " + resultSetConcurrency);
			}
			idStatement = connection.prepareStatement(prepareQuery(SELECT_ID_SQL), resultSetType,
					ResultSet.CONCUR_READ_ONLY);
			valuesStatement = connection.prepareStatement(prepareQuery(SELECT_VALUES_SQL), resultSetType,
					resultSetConcurrency);
			if (!readonlyDocuments && valuesStatement.getResultSetConcurrency() != ResultSet.CONCUR_UPDATABLE) {
				if (hasValue(exportColumnSuffix)) {
					updateStatements = prepareStatements(UPDATE_VALUES_SQL, contentColumns, exportColumnSuffix);
				} else {
					updateStatements = prepareStatements(UPDATE_VALUES_SQL, contentColumns);
				}
				if (!featureColumns.isEmpty()) {
					updateStatements.putAll(prepareStatements(UPDATE_VALUES_SQL, featureColumns));
				}
			}
			idStatement.setFetchDirection(fetchDirection);
			idStatement.setFetchSize(fetchIds);
			valuesStatement.setFetchDirection(fetchDirection);
			valuesStatement.setFetchSize(fetchRows);
			idResultSet = idStatement.executeQuery();
			valuesResultSet = valuesStatement.executeQuery();
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not prepare statement", e);
		}

		initVirtualCorpus();

		return this;
	}

	@Override
	public void cleanup() {
		try {
			if (!readonlyDocuments && valuesResultSet.getConcurrency() == ResultSet.CONCUR_UPDATABLE) {
				valuesResultSet.updateRow();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			throw new GateRuntimeException(e);
		}
		cleanupVirtualCorpus();
	}

	@Override
	protected int loadSize() throws Exception {
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(prepareQuery(COUNT_ID_SQL));
			resultSet.next();
			int rowCount = resultSet.getInt(1);
			int columnCount = contentColumnList.size();
			int size = rowCount * columnCount;
			return size;
		}
	}

	@Override
	protected String loadDocumentName(int index) throws Exception {
		Integer row = row(index);
		String contentColumn = column(index);

		Object id = getId(row);
		return buildDocumentName(id, contentColumn);
	}

	@Override
	protected Document loadDocument(int index) throws Exception {
		Integer row = row(index);
		String contentColumn = column(index);

		valuesResultSet = moveResultSetToRow(valuesStatement, valuesResultSet, row);

		Object id = valuesResultSet.getObject(idColumn);
		loadedIds.put(index, id);
		Object content = null;
		String encoding = null;
		String mimeType = null;
		if (hasValue(exportColumnSuffix)) {
			String exportColumn = exportColumnMapping.get(contentColumn);
			content = valuesResultSet.getObject(exportColumn);
			encoding = exportEncoding;
			mimeType = getExporter().getMimeType();
		}
		if (content == null) {
			content = valuesResultSet.getObject(contentColumn);
			encoding = this.encoding;
			mimeType = this.mimeType;
		}
		if (content == null) {
			content = "";
		} else if (content instanceof byte[]) {
			content = new String((byte[]) content, encoding);
		} else if (!(content instanceof String)) {
			content = content.toString();
		}
		FeatureMap features = Factory.newFeatureMap();
		for (String featureColumn : featureColumnList) {
			Object feature = valuesResultSet.getObject(featureColumn);
			features.put(featureColumn, feature);
		}
		FeatureMap params = Factory.newFeatureMap();
		params.put(Document.DOCUMENT_STRING_CONTENT_PARAMETER_NAME, content);
		params.put(Document.DOCUMENT_ENCODING_PARAMETER_NAME, encoding);
		params.put(Document.DOCUMENT_MIME_TYPE_PARAMETER_NAME, mimeType);
		String documentName = buildDocumentName(id, contentColumn);
		return (Document) Factory.createResource(DocumentImpl.class.getName(), params, features, documentName);
	}

	@Override
	protected Integer addDocuments(int index, Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void setDocument(int index, Document document) throws Exception {
		Integer row = row(index);
		String column = column(index);

		if (hasValue(exportColumnSuffix)) {
			column = exportColumnMapping.get(column);
		}

		byte[] bytes = export(getExporter(), document);

		if (valuesResultSet.getConcurrency() == ResultSet.CONCUR_UPDATABLE) {
			valuesResultSet = moveResultSetToRow(valuesStatement, valuesResultSet, row);
			valuesResultSet.updateBytes(column, bytes);
			if (!connection.getMetaData().ownUpdatesAreVisible(valuesResultSet.getType())) {
				valuesResultSet.updateRow();
				valuesResultSet.close();
			}
		} else {
			Object id = getId(row);
			PreparedStatement updateStatement = updateStatements.get(column);
			updateStatement.setBytes(1, bytes);
			updateStatement.setObject(2, id);
			updateStatement.executeUpdate();

			if (!connection.getMetaData().othersUpdatesAreVisible(idStatement.getResultSetType())) {
				idResultSet.close();
			}
			if (!connection.getMetaData().othersUpdatesAreVisible(valuesStatement.getResultSetType())) {
				valuesResultSet.close();
			}
		}
	}

	@Override
	protected Integer deleteDocuments(Collection<? extends Document> documents) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void deleteAllDocuments() throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void renameDocument(Document document, String oldName, String newName) throws Exception {
		throw new GateRuntimeException("renaming document is not supported");
	}

	@Override
	protected void documentUnloaded(int index, Document document) {
		int row = row(index);
		loadedIds.remove(row);
	}

	private Object getId(Integer row) throws SQLException {
		Object id = loadedIds.get(row);
		if (id != null) {
			return id;
		}

		idResultSet = moveResultSetToRow(idStatement, idResultSet, row);

		id = idResultSet.getObject(1);
		loadedIds.put(row, id);
		return id;
	}

	private String buildDocumentName(Object id, String contentColumn) {
		return id + " " + contentColumn;
	}

	private int row(int index) {
		return (index / contentColumnList.size()) + 1;
	}

	private String column(int index) {
		return contentColumnList.get(index % contentColumnList.size());
	}

	private String prepareQuery(String query) {
		query = query.replaceAll(Pattern.quote("${tableName}"), tableName);
		query = query.replaceAll(Pattern.quote("${idColumn}"), idColumn);
		query = query.replaceAll(Pattern.quote("${columns}"), String.join(",", this.columns));
		return query;
	}

	private PreparedStatement prepareStatement(String query, String column) throws SQLException {
		String columnQuery = query.replaceAll(Pattern.quote("${column}"), column);
		PreparedStatement statement = connection.prepareStatement(prepareQuery(columnQuery));
		return statement;
	}

	private Map<String, PreparedStatement> prepareStatements(String query, List<String> columns) throws SQLException {
		Map<String, PreparedStatement> statements = new HashMap<>();
		for (String column : columns) {
			statements.put(column, prepareStatement(query, column));
		}
		return statements;
	}

	private Map<String, PreparedStatement> prepareStatements(String query, List<String> columns, String suffix)
			throws SQLException {
		Map<String, PreparedStatement> statements = new HashMap<>();
		for (String column : columns) {
			statements.put(column + suffix, prepareStatement(query, column + suffix));
		}
		return statements;
	}

	private List<String> getTableColumnNames(String tableName) throws SQLException {
		try (ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null)) {
			List<String> columns = new ArrayList<>();
			while (resultSet.next()) {
				columns.add(resultSet.getString("COLUMN_NAME"));
			}
			return columns;
		}
	}

	private ResultSet moveResultSetToRow(PreparedStatement statement, ResultSet resultSet, Integer row)
			throws SQLException {
		boolean reopened = false;
		if (resultSet.isClosed()) {
			resultSet = statement.executeQuery();
			reopened = true;
		}
		int currentRow = resultSet.getRow();
		if (currentRow != row) {
			if (!reopened && resultSet.getConcurrency() == ResultSet.CONCUR_UPDATABLE) {
				resultSet.updateRow();
			}
			if (currentRow > row && resultSet.getType() == ResultSet.TYPE_FORWARD_ONLY) {
				resultSet.close();
				resultSet = statement.executeQuery();
				reopened = true;
			}
			resultSet.absolute(row);
		}
		return resultSet;
	}

}