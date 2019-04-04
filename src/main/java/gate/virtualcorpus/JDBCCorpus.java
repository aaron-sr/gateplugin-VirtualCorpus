package gate.virtualcorpus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

@CreoleResource(name = "JDBCCorpus", interfaceName = "gate.Corpus", icon = "corpus", comment = "A corpus backed by GATE documents stored in a JDBC table")
public class JDBCCorpus extends VirtualCorpus implements Corpus {
	private static final long serialVersionUID = -8485133333415382902L;
	private static Logger logger = Logger.getLogger(JDBCCorpus.class);

	public static final String FEATURE_JDBC_ID = "jdbcId";
	public static final String FEATURE_JDBC_CONTENT_COLUMN = "jdbcContentColumn";

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
	protected Integer fetchIds;
	protected Integer fetchRows;
	protected String exportColumnSuffix;

	private Connection connection;
	private Collection<String> allTableColumns;
	private List<String> columns;
	private List<String> contentColumnList;
	private List<String> featureColumnList;

	private PreparedStatement countIdStatement;
	private PreparedStatement selectIdStatement;
	private ResultSet selectIdResults;
	private PreparedStatement selectValuesStatement;
	private ResultSet selectValuesResults;
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
	@CreoleParameter(comment = "fetch n rows for document ids (i.e. JDBC fetch size)", defaultValue = "0")
	public void setFetchIds(Integer fetchIds) {
		this.fetchIds = fetchIds;
	}

	public Integer getFetchIds() {
		return fetchIds;
	}

	@Optional
	@CreoleParameter(comment = "fetch n rows for documents (i.e. JDBC fetch size)", defaultValue = "0")
	public void setFetchRows(Integer fetchRows) {
		this.fetchRows = fetchRows;
	}

	public Integer getFetchRows() {
		return fetchRows;
	}

	@Optional
	@CreoleParameter(comment = "suffix for value columns, where exported data is written (non-existent columns will be created)", defaultValue = "")
	public void setExportColumnSuffix(String exportColumnSuffix) {
		this.exportColumnSuffix = exportColumnSuffix;
	}

	public String getExportColumnSuffix() {
		return exportColumnSuffix;
	}

	@Override
	public Resource init() throws ResourceInstantiationException {
		checkValidMimeType();
		checkValidExporterClassName();
		if (!immutableCorpus) {
			throw new ResourceInstantiationException("unimmutable jdbc corpus currently not supported");
		}
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
		if (featureColumns.contains(FEATURE_JDBC_ID)) {
			throw new ResourceInstantiationException("featureColumns cannot contain " + FEATURE_JDBC_ID);
		}
		if (featureColumns.contains(FEATURE_JDBC_CONTENT_COLUMN)) {
			throw new ResourceInstantiationException("featureColumns cannot contain " + FEATURE_JDBC_CONTENT_COLUMN);
		}
		if (contentColumns.contains(ALL_COLUMNS)) {
			List<String> columns = new ArrayList<String>(allTableColumns);
			columns.remove(idColumn);
			columns.removeAll(featureColumns);
			if (hasValue(exportColumnSuffix)) {
				Iterator<String> iterator = columns.iterator();
				while (iterator.hasNext()) {
					String column = iterator.next();
					if (column.endsWith(exportColumnSuffix)
							&& columns.contains(column.substring(0, column.length() - exportColumnSuffix.length()))) {
						iterator.remove();
					}
				}
			}
			contentColumns.clear();
			contentColumns.addAll(columns);
		}
		if (!allTableColumns.containsAll(contentColumns)) {
			contentColumns.removeAll(allTableColumns);
			throw new ResourceInstantiationException("content columns does not exist: " + contentColumns);
		}
		if (!allTableColumns.containsAll(featureColumns)) {
			featureColumns.removeAll(allTableColumns);
			throw new ResourceInstantiationException("feature columns does not exist: " + featureColumns);
		}
		this.contentColumnList = contentColumns;
		this.featureColumnList = featureColumns;
		this.columns = new ArrayList<>();
		this.columns.addAll(contentColumns);
		this.columns.addAll(featureColumns);

		try {
			countIdStatement = connection.prepareStatement(prepareQuery(COUNT_ID_SQL));

			if (connection.getMetaData().supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY)) {
				selectIdStatement = connection.prepareStatement(prepareQuery(SELECT_ID_SQL),
						ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				selectValuesStatement = connection.prepareStatement(prepareQuery(SELECT_VALUES_SQL),
						ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			} else {
				selectValuesStatement = connection.prepareStatement(prepareQuery(SELECT_VALUES_SQL));
				selectIdStatement = connection.prepareStatement(prepareQuery(SELECT_ID_SQL));
			}
			selectIdStatement.setFetchSize(fetchIds);
			selectValuesStatement.setFetchSize(fetchRows);
			selectIdResults = selectIdStatement.executeQuery();
			selectValuesResults = selectValuesStatement.executeQuery();

			if (!readonlyDocuments) {
				if (hasValue(exportColumnSuffix)) {
					for (String contentColumn : contentColumns) {
						String exportColumn = contentColumn + exportColumnSuffix;
						if (!allTableColumns.contains(exportColumn)) {
							throw new ResourceInstantiationException(
									"export content columns does not exist: " + exportColumn);
						}
					}
					updateStatements = prepareStatements(UPDATE_VALUES_SQL, contentColumns, exportColumnSuffix);
				} else {
					updateStatements = prepareStatements(UPDATE_VALUES_SQL, contentColumns);
				}
				if (!featureColumns.isEmpty()) {
					updateStatements.putAll(prepareStatements(UPDATE_VALUES_SQL, featureColumns));
				}
			}
		} catch (SQLException e) {
			throw new ResourceInstantiationException("Could not prepare statement", e);
		}

		initVirtualCorpus();

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
		cleanupVirtualCorpus();
	}

	@Override
	protected int loadSize() throws Exception {
		try (ResultSet resultSet = countIdStatement.executeQuery()) {
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

		if (selectValuesResults.isClosed()) {
			selectValuesResults = selectValuesStatement.executeQuery();
		}
		int currentRow = selectIdResults.getRow();
		if (currentRow != row) {
			if (currentRow > row && selectValuesStatement.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
				selectValuesResults.close();
				selectValuesResults = selectValuesStatement.executeQuery();
			}
			selectValuesResults.absolute(row);
		}

		Object id = selectValuesResults.getObject(idColumn);
		Object content = selectValuesResults.getObject(contentColumn);
		if (content == null) {
			content = "";
		} else if (content instanceof byte[]) {
			content = new String((byte[]) content, encoding);
		} else if (!(content instanceof String)) {
			content = content.toString();
		}
		FeatureMap features = Factory.newFeatureMap();
		for (String featureColumn : featureColumnList) {
			Object feature = selectValuesResults.getObject(featureColumn);
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

		Object id = getId(row);

		if (hasValue(exportColumnSuffix)) {
			column = column + exportColumnSuffix;
		}

		updateColumnContent(id, column, export(getExporter(), document));
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

		if (selectIdResults.isClosed()) {
			selectIdResults = selectIdStatement.executeQuery();
		}
		int currentRow = selectIdResults.getRow();
		if (currentRow != row) {
			if (currentRow > row && selectIdStatement.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
				selectIdResults.close();
				selectIdResults = selectIdStatement.executeQuery();
			}

			selectIdResults.absolute(row);
		}

		id = selectIdResults.getObject(1);
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

	private void updateColumnContent(Object id, String column, Object value) throws SQLException {
		PreparedStatement updateStatement = updateStatements.get(column);
		setValue(updateStatement, 1, value);
		if (value instanceof byte[]) {
			updateStatement.setBytes(1, (byte[]) value);
		} else {
			updateStatement.setObject(1, value);
		}
		updateStatement.setObject(2, id);
		updateStatement.executeUpdate();

		if (!connection.getMetaData().othersUpdatesAreVisible(selectIdStatement.getResultSetType())) {
			selectIdResults.close();
		}
		if (!connection.getMetaData().othersUpdatesAreVisible(selectValuesStatement.getResultSetType())) {
			selectValuesResults.close();
		}
	}

	private void setValue(PreparedStatement statement, int index, Object value) throws SQLException {
		if (value == null) {
			statement.setNull(index, JDBCType.NULL.getVendorTypeNumber());
		} else if (value instanceof Integer) {
			statement.setInt(index, (Integer) value);
		} else if (value instanceof Long) {
			statement.setLong(index, (Long) value);
		} else if (value instanceof Float) {
			statement.setFloat(index, (Float) value);
		} else if (value instanceof Double) {
			statement.setDouble(index, (Double) value);
		} else if (value instanceof String) {
			statement.setString(index, (String) value);
		} else if (value instanceof byte[]) {
			statement.setBytes(index, (byte[]) value);
		} else {
			statement.setObject(index, value);
		}
	}

}