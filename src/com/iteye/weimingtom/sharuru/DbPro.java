/**
 * Copyright (c) 2011-2015, James Zhan 詹波 (jfinal@126.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.iteye.weimingtom.sharuru;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * DbPro. Professional database query and update tool.
 * 
 * @see Db
 * @see DbKit
 * @see TableMapping
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DbPro {
	//-------------------------------------
	//DbKit
	
	/**
	 * The main Config object for system
	 */
	public static Config config_ = null;
	
	/**
	 * For Model.getAttrsMap()/getModifyFlag() and Record.getColumnsMap()
	 * while the ActiveRecordPlugin not start or the Exception throws of HashSessionManager.restorSession(..) by Jetty
	 */
	static Config brokenConfig = new Config();
	
	private static Map<Class<? extends Model>, Config> modelToConfig = new HashMap<Class<? extends Model>, Config>();
	private static Map<String, Config> configNameToConfig = new HashMap<String, Config>();
	
	static final Object[] NULL_PARA_ARRAY = new Object[0];
	public static final String MAIN_CONFIG_NAME = "main";
	
	
	//start
	public static boolean startActiveRecordPlugin(Config config) throws SQLException {
		if (config.isStarted)
			return true;
		
		if (isBlank(config.name))
			throw new IllegalArgumentException("Config name can not be blank");
		if (config.dataSource == null)
			throw new IllegalArgumentException("DataSource can not be null");
		
		config.name = config.name.trim();

		DbPro.addConfig(config);
		
		boolean succeed = config.build();
		if (succeed) {
			config.isStarted = true;
		}
		return succeed;
	}
	
	private static boolean isBlank(String str) {
		return str == null || "".equals(str.trim()) ? true : false;
	}
	
	public static boolean stopActiveRecordPlugin(Config config) {
		config.isStarted = false;
		return true;
	}
	
	
	/**
	 * Add Config object
	 * @param config the Config contains DataSource, Dialect and so on
	 */
	public static void addConfig(Config config) {
		if (config == null)
			throw new IllegalArgumentException("Config can not be null");
		if (configNameToConfig.containsKey(config.getName()))
			throw new IllegalArgumentException("Config already exists: " + config.getName());
		
		configNameToConfig.put(config.getName(), config);
		
		/** 
		 * Replace the main config if current config name is MAIN_CONFIG_NAME
		 */
		if (MAIN_CONFIG_NAME.equals(config.getName()))
			DbPro.config_ = config;
		
		/**
		 * The configName may not be MAIN_CONFIG_NAME,
		 * the main config have to set the first comming Config if it is null
		 */
		if (DbPro.config_ == null)
			DbPro.config_ = config;
	}
	
	public static void addModelToConfigMapping(Class<? extends Model> modelClass, Config config) {
		modelToConfig.put(modelClass, config);
	}
	
	public static Config getConfig() {
		return config_;
	}
	
	public static Config getConfig(String configName) {
		return configNameToConfig.get(configName);
	}
	
	public static Config getConfig(Class<? extends Model> modelClass) {
		return modelToConfig.get(modelClass);
	}
	
	
	//-------------------------------------
	//TableMapping
	
	private final static Map<Class<? extends Model<?>>, Table> modelToTableMap = new HashMap<Class<? extends Model<?>>, Table>();
	
	public static void putTable(Table table) {
		modelToTableMap.put(table.getModelClass(), table);
	}
	
	public static Table getTable(Class<? extends Model> modelClass) {
		Table table = modelToTableMap.get(modelClass);
		if (table == null)
			throw new RuntimeException("The Table mapping of model: " + modelClass.getName() + " not exists. Please add mapping to ActiveRecordPlugin: activeRecordPlugin.addMapping(tableName, YourModel.class).");
		
		return table;
	}
	
	//-------------------------------------
	
	private final Config config;
	private static final Map<String, DbPro> map = new HashMap<String, DbPro>();
	
	public DbPro() {
		if (DbPro.config_ == null)
			throw new RuntimeException("The main config is null, initialize ActiveRecordPlugin first");
		this.config = DbPro.config_;
	}
	
	public DbPro(String configName) {
		DbPro.config_ = DbPro.getConfig(configName);
		if (DbPro.config_ == null)
			throw new IllegalArgumentException("Config not found by configName: " + configName);
		this.config = DbPro.config_;
	}
	
	public static DbPro use(String configName) {
		DbPro result = map.get(configName);
		if (result == null) {
			result = new DbPro(configName);
			map.put(configName, result);
		}
		return result;
	}
	
	public static DbPro use() {
		return use(DbPro.config_.name);
	}
	
	<T> List<T> query(Config config, Connection conn, String sql, Object... paras) throws SQLException {
		List result = new ArrayList();
		PreparedStatement pst = conn.prepareStatement(sql);
		fillStatement(pst, paras);
		ResultSet rs = pst.executeQuery();
		int colAmount = rs.getMetaData().getColumnCount();
		if (colAmount > 1) {
			while (rs.next()) {
				Object[] temp = new Object[colAmount];
				for (int i=0; i<colAmount; i++) {
					temp[i] = rs.getObject(i + 1);
				}
				result.add(temp);
			}
		}
		else if(colAmount == 1) {
			while (rs.next()) {
				result.add(rs.getObject(1));
			}
		}
		closeQuietly(rs, pst);
		return result;
	}
	
	/**
	 * @throws SQLException 
	 * @see #query(String, String, Object...)
	 */
	public <T> List<T> query(String sql, Object... paras) throws SQLException {
		Connection conn = null;
		try {
			conn = config.getConnection();
			return query(config, conn, sql, paras);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * @see #query(String, Object...)
	 * @param sql an SQL statement
	 * @throws SQLException 
	 */
	public <T> List<T> query(String sql) throws SQLException {		// return  List<object[]> or List<object>
		return query(sql, NULL_PARA_ARRAY);
	}
	
	/**
	 * Execute sql query and return the first result. I recommend add "limit 1" in your sql.
	 * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param paras the parameters of sql
	 * @return Object[] if your sql has select more than one column,
	 * 			and it return Object if your sql has select only one column.
	 * @throws SQLException 
	 */
	public <T> T queryFirst(String sql, Object... paras) throws SQLException {
		List<T> result = query(sql, paras);
		return (result.size() > 0 ? result.get(0) : null);
	}
	
	/**
	 * @see #queryFirst(String, Object...)
	 * @param sql an SQL statement
	 * @throws SQLException 
	 */
	public <T> T queryFirst(String sql) throws SQLException {
		// return queryFirst(sql, NULL_PARA_ARRAY);
		List<T> result = query(sql, NULL_PARA_ARRAY);
		return (result.size() > 0 ? result.get(0) : null);
	}
	
	// 26 queryXxx method below -----------------------------------------------
	/**
	 * Execute sql query just return one column.
	 * @param <T> the type of the column that in your sql's select statement
	 * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param paras the parameters of sql
	 * @return List<T>
	 * @throws SQLException 
	 */
	public <T> T queryColumn(String sql, Object... paras) throws SQLException {
		List<T> result = query(sql, paras);
		if (result.size() > 0) {
			T temp = result.get(0);
			if (temp instanceof Object[]) {
				throw new IllegalArgumentException("Only ONE COLUMN can be queried.");
			}
			return temp;
		}
		return null;
	}
	
	public <T> T queryColumn(String sql) throws SQLException {
		return (T)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public String queryStr(String sql, Object... paras) throws SQLException {
		return (String)queryColumn(sql, paras);
	}
	
	public String queryStr(String sql) throws SQLException {
		return (String)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public Integer queryInt(String sql, Object... paras) throws SQLException {
		return (Integer)queryColumn(sql, paras);
	}
	
	public Integer queryInt(String sql) throws SQLException {
		return (Integer)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public Long queryLong(String sql, Object... paras) throws SQLException {
		return (Long)queryColumn(sql, paras);
	}
	
	public Long queryLong(String sql) throws SQLException {
		return (Long)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public Double queryDouble(String sql, Object... paras) throws SQLException {
		return (Double)queryColumn(sql, paras);
	}
	
	public Double queryDouble(String sql) throws SQLException {
		return (Double)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public Float queryFloat(String sql, Object... paras) throws SQLException {
		return (Float)queryColumn(sql, paras);
	}
	
	public Float queryFloat(String sql) throws SQLException {
		return (Float)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public java.math.BigDecimal queryBigDecimal(String sql, Object... paras) throws SQLException {
		return (java.math.BigDecimal)queryColumn(sql, paras);
	}
	
	public java.math.BigDecimal queryBigDecimal(String sql) throws SQLException {
		return (java.math.BigDecimal)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public byte[] queryBytes(String sql, Object... paras) throws SQLException {
		return (byte[])queryColumn(sql, paras);
	}
	
	public byte[] queryBytes(String sql) throws SQLException {
		return (byte[])queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public java.util.Date queryDate(String sql, Object... paras) throws SQLException {
		return (java.util.Date)queryColumn(sql, paras);
	}
	
	public java.util.Date queryDate(String sql) throws SQLException {
		return (java.util.Date)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public java.sql.Time queryTime(String sql, Object... paras) throws SQLException {
		return (java.sql.Time)queryColumn(sql, paras);
	}
	
	public java.sql.Time queryTime(String sql) throws SQLException {
		return (java.sql.Time)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public java.sql.Timestamp queryTimestamp(String sql, Object... paras) throws SQLException {
		return (java.sql.Timestamp)queryColumn(sql, paras);
	}
	
	public java.sql.Timestamp queryTimestamp(String sql) throws SQLException {
		return (java.sql.Timestamp)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public Boolean queryBoolean(String sql, Object... paras) throws SQLException {
		return (Boolean)queryColumn(sql, paras);
	}
	
	public Boolean queryBoolean(String sql) throws SQLException {
		return (Boolean)queryColumn(sql, NULL_PARA_ARRAY);
	}
	
	public Number queryNumber(String sql, Object... paras) throws SQLException {
		return (Number)queryColumn(sql, paras);
	}
	
	public Number queryNumber(String sql) throws SQLException {
		return (Number)queryColumn(sql, NULL_PARA_ARRAY);
	}
	// 26 queryXxx method under -----------------------------------------------
	
	/**
	 * Execute sql update
	 */
	int update(Config config, Connection conn, String sql, Object... paras) throws SQLException {
		PreparedStatement pst = conn.prepareStatement(sql);
		fillStatement(pst, paras);
		int result = pst.executeUpdate();
		closeQuietly(pst);
		return result;
	}
	
	/**
	 * Execute update, insert or delete sql statement.
	 * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param paras the parameters of sql
	 * @return either the row count for <code>INSERT</code>, <code>UPDATE</code>,
     *         or <code>DELETE</code> statements, or 0 for SQL statements 
     *         that return nothing
	 * @throws SQLException 
	 */
	public int update(String sql, Object... paras) throws SQLException {
		Connection conn = null;
		try {
			conn = config.getConnection();
			return update(config, conn, sql, paras);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * @see #update(String, Object...)
	 * @param sql an SQL statement
	 * @throws SQLException 
	 */
	public int update(String sql) throws SQLException {
		return update(sql, NULL_PARA_ARRAY);
	}
	
	/**
	 * Get id after insert method getGeneratedKey().
	 */
	private Object getGeneratedKey(PreparedStatement pst) throws SQLException {
		ResultSet rs = pst.getGeneratedKeys();
		Object id = null;
		if (rs.next())
			 id = rs.getObject(1);
		rs.close();
		return id;
	}
	
	List<Record> find(Config config, Connection conn, String sql, Object... paras) throws SQLException {
		PreparedStatement pst = conn.prepareStatement(sql);
		fillStatement(pst, paras);
		ResultSet rs = pst.executeQuery();
		List<Record> result = build(config, rs);
		closeQuietly(rs, pst);
		return result;
	}
	
	/**
	 * @throws SQLException 
	 * @see #find(String, String, Object...)
	 */
	public List<Record> find(String sql, Object... paras) throws SQLException {
		Connection conn = null;
		try {
			conn = config.getConnection();
			return find(config, conn, sql, paras);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * @see #find(String, String, Object...)
	 * @param sql the sql statement
	 * @throws SQLException 
	 */
	public List<Record> find(String sql) throws SQLException {
		return find(sql, NULL_PARA_ARRAY);
	}
	
	/**
	 * Find first record. I recommend add "limit 1" in your sql.
	 * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param paras the parameters of sql
	 * @return the Record object
	 * @throws SQLException 
	 */
	public Record findFirst(String sql, Object... paras) throws SQLException {
		List<Record> result = find(sql, paras);
		return result.size() > 0 ? result.get(0) : null;
	}
	
	/**
	 * @see #findFirst(String, Object...)
	 * @param sql an SQL statement
	 * @throws SQLException 
	 */
	public Record findFirst(String sql) throws SQLException {
		List<Record> result = find(sql, NULL_PARA_ARRAY);
		return result.size() > 0 ? result.get(0) : null;
	}
	
	/**
	 * Find record by id.
	 * Example: Record user = DbPro.use().findById("user", 15);
	 * @param tableName the table name of the table
	 * @param idValue the id value of the record
	 * @throws SQLException 
	 */
	public Record findById(String tableName, Object idValue) throws SQLException {
		return findById(tableName, getDefaultPrimaryKey(), idValue, "*");
	}
	
	/**
	 * Find record by id. Fetch the specific columns only.
	 * Example: Record user = DbPro.use().findById("user", 15, "name, age");
	 * @param tableName the table name of the table
	 * @param idValue the id value of the record
	 * @param columns the specific columns separate with comma character ==> ","
	 * @throws SQLException 
	 */
	public Record findById(String tableName, Number idValue, String columns) throws SQLException {
		return findById(tableName, getDefaultPrimaryKey(), idValue, columns);
	}
	
	/**
	 * Find record by id.
	 * Example: Record user = DbPro.use().findById("user", "user_id", 15);
	 * @param tableName the table name of the table
	 * @param primaryKey the primary key of the table
	 * @param idValue the id value of the record
	 * @throws SQLException 
	 */
	public Record findById(String tableName, String primaryKey, Number idValue) throws SQLException {
		return findById(tableName, primaryKey, idValue, "*");
	}
	
	/**
	 * Find record by id. Fetch the specific columns only.
	 * Example: Record user = DbPro.use().findById("user", "user_id", 15, "name, age");
	 * @param tableName the table name of the table
	 * @param primaryKey the primary key of the table
	 * @param idValue the id value of the record
	 * @param columns the specific columns separate with comma character ==> ","
	 * @throws SQLException 
	 */
	public Record findById(String tableName, String primaryKey, Object idValue, String columns) throws SQLException {
		String sql = forDbFindById(tableName, primaryKey, columns);
		List<Record> result = find(sql, idValue);
		return result.size() > 0 ? result.get(0) : null;
	}
	
	/**
	 * Delete record by id.
	 * Example: boolean succeed = DbPro.use().deleteById("user", 15);
	 * @param tableName the table name of the table
	 * @param id the id value of the record
	 * @return true if delete succeed otherwise false
	 * @throws SQLException 
	 */
	public boolean deleteById(String tableName, Object id) throws SQLException {
		return deleteById(tableName, getDefaultPrimaryKey(), id);
	}
	
	/**
	 * Delete record by id.
	 * Example: boolean succeed = DbPro.use().deleteById("user", "user_id", 15);
	 * @param tableName the table name of the table
	 * @param primaryKey the primary key of the table
	 * @param id the id value of the record
	 * @return true if delete succeed otherwise false
	 * @throws SQLException 
	 */
	public boolean deleteById(String tableName, String primaryKey, Object id) throws SQLException {
		if (id == null) {
			throw new IllegalArgumentException("id can not be null");
		}
		String sql = forDbDeleteById(tableName, primaryKey);
		return update(sql, id) >= 1;
	}
	
	/**
	 * Delete record.
	 * Example: boolean succeed = DbPro.use().delete("user", "id", user);
	 * @param tableName the table name of the table
	 * @param primaryKey the primary key of the table
	 * @param record the record
	 * @return true if delete succeed otherwise false
	 * @throws SQLException 
	 */
	public boolean delete(String tableName, String primaryKey, Record record) throws SQLException {
		return deleteById(tableName, primaryKey, record.get(primaryKey));
	}
	
	/**
	 * Example: boolean succeed = DbPro.use().delete("user", user);
	 * @throws SQLException 
	 * @see #delete(String, String, Record)
	 */
	public boolean delete(String tableName, Record record) throws SQLException {
		String defaultPrimaryKey = getDefaultPrimaryKey();
		return deleteById(tableName, defaultPrimaryKey, record.get(defaultPrimaryKey));
	}
	
	Page<Record> paginate(Config config, Connection conn, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
		if (pageNumber < 1 || pageSize < 1) {
			throw new IllegalArgumentException("pageNumber and pageSize must be more than 0");
		}
//		if (config.dialect.isTakeOverDbPaginate())
//			return config.dialect.takeOverDbPaginate(conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
		
		long totalRow = 0;
		int totalPage = 0;
		List result = query(config, conn, "select count(*) " + replaceFormatSqlOrderBy(sqlExceptSelect), paras);
		int size = result.size();
		if (size == 1)
			totalRow = ((Number)result.get(0)).longValue();
		else if (size > 1)
			totalRow = result.size();
		else
			return new Page<Record>(new ArrayList<Record>(0), pageNumber, pageSize, 0, 0);
		
		totalPage = (int) (totalRow / pageSize);
		if (totalRow % pageSize != 0) {
			totalPage++;
		}
		
		// --------
		StringBuilder sql = new StringBuilder();
		forPaginate(sql, pageNumber, pageSize, select, sqlExceptSelect);
		List<Record> list = find(config, conn, sql.toString(), paras);
		return new Page<Record>(list, pageNumber, pageSize, totalPage, (int)totalRow);
	}
	
	/**
	 * @throws SQLException 
	 * @see #paginate(String, int, int, String, String, Object...)
	 */
	public Page<Record> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
		Connection conn = null;
		try {
			conn = config.getConnection();
			return paginate(config, conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * @throws SQLException 
	 * @see #paginate(String, int, int, String, String, Object...)
	 */
	public Page<Record> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect) throws SQLException {
		return paginate(pageNumber, pageSize, select, sqlExceptSelect, NULL_PARA_ARRAY);
	}
	
	boolean save(Config config, Connection conn, String tableName, String primaryKey, Record record) throws SQLException {
		List<Object> paras = new ArrayList<Object>();
		StringBuilder sql = new StringBuilder();
		forDbSave(sql, paras, tableName, record);
		
		PreparedStatement pst;
			pst = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
		fillStatement(pst, paras);
		int result = pst.executeUpdate();
		record.set(primaryKey, getGeneratedKey(pst));
		closeQuietly(pst);
		return result >= 1;
	}
	
	/**
	 * Save record.
	 * @param tableName the table name of the table
	 * @param primaryKey the primary key of the table
	 * @param record the record will be saved
	 * @param true if save succeed otherwise false
	 * @throws SQLException 
	 */
	public boolean save(String tableName, String primaryKey, Record record) throws SQLException {
		Connection conn = null;
		try {
			conn = config.getConnection();
			return save(config, conn, tableName, primaryKey, record);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * @throws SQLException 
	 * @see #save(String, String, Record)
	 */
	public boolean save(String tableName, Record record) throws SQLException {
		return save(tableName, getDefaultPrimaryKey(), record);
	}
	
	boolean update(Config config, Connection conn, String tableName, String primaryKey, Record record) throws SQLException {
		Object id = record.get(primaryKey);
		if (id == null) {
			throw new IllegalArgumentException("You can't update model without Primary Key.");
		}
		StringBuilder sql = new StringBuilder();
		List<Object> paras = new ArrayList<Object>();
		forDbUpdate(tableName, primaryKey, id, record, sql, paras);
		
		if (paras.size() <= 1) {	// Needn't update
			return false;
		}
		
		return update(config, conn, sql.toString(), paras.toArray()) >= 1;
	}
	
	/**
	 * Update Record.
	 * @param tableName the table name of the Record save to
	 * @param primaryKey the primary key of the table
	 * @param record the Record object
	 * @param true if update succeed otherwise false
	 * @throws SQLException 
	 */
	public boolean update(String tableName, String primaryKey, Record record) throws SQLException {
		Connection conn = null;
		try {
			conn = config.getConnection();
			return update(config, conn, tableName, primaryKey, record);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * Update Record. The primary key of the table is: "id".
	 * @throws SQLException 
	 * @see #update(String, String, Record)
	 */
	public boolean update(String tableName, Record record) throws SQLException {
		return update(tableName, getDefaultPrimaryKey(), record);
	}
	
	/**
	 * Execute transaction.
	 * @param config the Config object
	 * @param transactionLevel the transaction level
	 * @param atom the atom operation
	 * @return true if transaction executing succeed otherwise false
	 * @throws SQLException 
	 */
	boolean tx(Config config, int transactionLevel, Runnable atom) throws SQLException {
		Connection conn = config.getThreadLocalConnection();
		if (conn != null) {	// Nested transaction support
			try {
				if (conn.getTransactionIsolation() < transactionLevel)
					conn.setTransactionIsolation(transactionLevel);
				atom.run();
				return true;
			} catch (SQLException e) {
				throw e;
			} catch (Throwable e) {
				throw new RuntimeException("Notice the outer transaction that the nested transaction return false", e);	// important:can not return false
			}
		}
		
		Boolean autoCommit = null;
		try {
			conn = config.getConnection();
			autoCommit = conn.getAutoCommit();
			config.setThreadLocalConnection(conn);
			conn.setTransactionIsolation(transactionLevel);
			conn.setAutoCommit(false);
			atom.run();
			conn.commit();
			return true;
		} catch (Throwable t) {
			if (conn != null) try {conn.rollback();} catch (Exception e1) {e1.printStackTrace();}
			throw t instanceof RuntimeException ? (RuntimeException)t : new RuntimeException(t);
		} finally {
			try {
				if (conn != null) {
					if (autoCommit != null)
						conn.setAutoCommit(autoCommit);
					conn.close();
				}
			} catch (Throwable t) {
				t.printStackTrace();	// can not throw exception here, otherwise the more important exception in previous catch block can not be thrown
			} finally {
				config.removeThreadLocalConnection();	// prevent memory leak
			}
		}
	}
	
	public boolean tx(int transactionLevel, Runnable atom) throws SQLException {
		return tx(config, transactionLevel, atom);
	}
	
	/**
	 * Execute transaction with default transaction level.
	 * @throws SQLException 
	 * @see #tx(int, IAtom)
	 */
	public boolean tx(Runnable atom) throws SQLException {
		return tx(config, config.getTransactionLevel(), atom);
	}
	
	private int[] batch(Config config, Connection conn, String sql, Object[][] paras, int batchSize) throws SQLException {
		if (paras == null || paras.length == 0)
			throw new IllegalArgumentException("The paras array length must more than 0.");
		if (batchSize < 1)
			throw new IllegalArgumentException("The batchSize must more than 0.");
		int counter = 0;
		int pointer = 0;
		int[] result = new int[paras.length];
		PreparedStatement pst = conn.prepareStatement(sql);
		for (int i=0; i<paras.length; i++) {
			for (int j=0; j<paras[i].length; j++) {
				Object value = paras[i][j];
				pst.setObject(j + 1, value);
			}
			pst.addBatch();
			if (++counter >= batchSize) {
				counter = 0;
				int[] r = pst.executeBatch();
				conn.commit();
				for (int k=0; k<r.length; k++)
					result[pointer++] = r[k];
			}
		}
		int[] r = pst.executeBatch();
		conn.commit();
		for (int k=0; k<r.length; k++)
			result[pointer++] = r[k];
		closeQuietly(pst);
		return result;
	}
	
    /**
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
     * <p>
     * Example:
     * <pre>
     * String sql = "insert into user(name, cash) values(?, ?)";
     * int[] result = DbPro.use().batch("myConfig", sql, new Object[][]{{"James", 888}, {"zhanjin", 888}});
     * </pre>
     * @param sql The SQL to execute.
     * @param paras An array of query replacement parameters.  Each row in this array is one set of batch replacement values.
     * @return The number of rows updated per statement
     * @throws SQLException 
     */
	public int[] batch(String sql, Object[][] paras, int batchSize) throws SQLException {
		Connection conn = null;
		Boolean autoCommit = null;
		try {
			conn = config.getConnection();
			autoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			return batch(config, conn, sql, paras, batchSize);
		} finally {
			if (autoCommit != null)
				try {conn.setAutoCommit(autoCommit);} catch (Exception e) {e.printStackTrace();}
			config.close(conn);
		}
	}
	
	private int[] batch(Config config, Connection conn, String sql, String columns, List list, int batchSize) throws SQLException {
		if (list == null || list.size() == 0)
			return new int[0];
		Object element = list.get(0);
		if (!(element instanceof Record) && !(element instanceof Model))
			throw new IllegalArgumentException("The element in list must be Model or Record.");
		if (batchSize < 1)
			throw new IllegalArgumentException("The batchSize must more than 0.");
		boolean isModel = element instanceof Model;
		
		String[] columnArray = columns.split(",");
		for (int i=0; i<columnArray.length; i++)
			columnArray[i] = columnArray[i].trim();
		
		int counter = 0;
		int pointer = 0;
		int size = list.size();
		int[] result = new int[size];
		PreparedStatement pst = conn.prepareStatement(sql);
		for (int i=0; i<size; i++) {
			Map map = isModel ? ((Model)list.get(i)).getAttrs() : ((Record)list.get(i)).getColumns();
			for (int j=0; j<columnArray.length; j++) {
				Object value = map.get(columnArray[j]);
				pst.setObject(j + 1, value);
			}
			pst.addBatch();
			if (++counter >= batchSize) {
				counter = 0;
				int[] r = pst.executeBatch();
				conn.commit();
				for (int k=0; k<r.length; k++)
					result[pointer++] = r[k];
			}
		}
		int[] r = pst.executeBatch();
		conn.commit();
		for (int k=0; k<r.length; k++)
			result[pointer++] = r[k];
		closeQuietly(pst);
		return result;
	}
	
	/**
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
     * <p>
     * Example:
     * <pre>
     * String sql = "insert into user(name, cash) values(?, ?)";
     * int[] result = DbPro.use().batch("myConfig", sql, "name, cash", modelList, 500);
     * </pre>
	 * @param sql The SQL to execute.
	 * @param columns the columns need be processed by sql.
	 * @param modelOrRecordList model or record object list.
	 * @param batchSize batch size.
	 * @return The number of rows updated per statement
	 * @throws SQLException 
	 */
	public int[] batch(String sql, String columns, List modelOrRecordList, int batchSize) throws SQLException {
		Connection conn = null;
		Boolean autoCommit = null;
		try {
			conn = config.getConnection();
			autoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			return batch(config, conn, sql, columns, modelOrRecordList, batchSize);
		} finally {
			if (autoCommit != null)
				try {conn.setAutoCommit(autoCommit);} catch (Exception e) {e.printStackTrace();}
			config.close(conn);
		}
	}
	
	private int[] batch(Config config, Connection conn, List<String> sqlList, int batchSize) throws SQLException {
		if (sqlList == null || sqlList.size() == 0)
			throw new IllegalArgumentException("The sqlList length must more than 0.");
		if (batchSize < 1)
			throw new IllegalArgumentException("The batchSize must more than 0.");
		int counter = 0;
		int pointer = 0;
		int size = sqlList.size();
		int[] result = new int[size];
		Statement st = conn.createStatement();
		for (int i=0; i<size; i++) {
			st.addBatch(sqlList.get(i));
			if (++counter >= batchSize) {
				counter = 0;
				int[] r = st.executeBatch();
				conn.commit();
				for (int k=0; k<r.length; k++)
					result[pointer++] = r[k];
			}
		}
		int[] r = st.executeBatch();
		conn.commit();
		for (int k=0; k<r.length; k++)
			result[pointer++] = r[k];
		closeQuietly(st);
		return result;
	}
	
    /**
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
     * Example:
     * <pre>
     * int[] result = DbPro.use().batch("myConfig", sqlList, 500);
     * </pre>
	 * @param sqlList The SQL list to execute.
	 * @param batchSize batch size.
	 * @return The number of rows updated per statement
     * @throws SQLException 
	 */
    public int[] batch(List<String> sqlList, int batchSize) throws SQLException {
		Connection conn = null;
		Boolean autoCommit = null;
		try {
			conn = config.getConnection();
			autoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			return batch(config, conn, sqlList, batchSize);
		} finally {
			if (autoCommit != null)
				try {conn.setAutoCommit(autoCommit);} catch (Exception e) {e.printStackTrace();}
			config.close(conn);
		}
    }
    
    
    private static final void closeQuietly(Statement st) {
		if (st != null) {try {st.close();} catch (SQLException e) {}}
	}
    
    private static final void closeQuietly(ResultSet rs, Statement st) {
		if (rs != null) {try {rs.close();} catch (SQLException e) {}}
		if (st != null) {try {st.close();} catch (SQLException e) {}}
	}
    
	private static String replaceFormatSqlOrderBy(String sql) {
		sql = sql.replaceAll("(\\s)+", " ");
		int index = sql.toLowerCase().lastIndexOf("order by");
		if (index > sql.toLowerCase().lastIndexOf(")")) {
			String sql1 = sql.substring(0, index);
			String sql2 = sql.substring(index);
			sql2 = sql2.replaceAll("[oO][rR][dD][eE][rR] [bB][yY] [\u4e00-\u9fa5a-zA-Z0-9_.]+((\\s)+(([dD][eE][sS][cC])|([aA][sS][cC])))?(( )*,( )*[\u4e00-\u9fa5a-zA-Z0-9_.]+(( )+(([dD][eE][sS][cC])|([aA][sS][cC])))?)*", "");
			return sql1 + sql2;
		}
		return sql;
	}
 
	
   	public static final List<Record> build(Config config, ResultSet rs) throws SQLException {
		List<Record> result = new ArrayList<Record>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		String[] labelNames = new String[columnCount + 1];
		int[] types = new int[columnCount + 1];
		buildLabelNamesAndTypes(rsmd, labelNames, types);
		while (rs.next()) {
			Record record = new Record();
			record.setColumnsMap(new HashMap<String, Object>());
			Map<String, Object> columns = record.getColumns();
			for (int i=1; i<=columnCount; i++) {
				Object value;
				if (types[i] < Types.BLOB)
					value = rs.getObject(i);
				else if (types[i] == Types.CLOB)
					value = handleClob(rs.getClob(i));
				else if (types[i] == Types.NCLOB)
					value = handleClob(rs.getNClob(i));
				else if (types[i] == Types.BLOB)
					value = handleBlob(rs.getBlob(i));
				else
					value = rs.getObject(i);
				
				columns.put(labelNames[i], value);
			}
			result.add(record);
		}
		return result;
	}
   	
	private static final void buildLabelNamesAndTypes(ResultSetMetaData rsmd, String[] labelNames, int[] types) throws SQLException {
		for (int i=1; i<labelNames.length; i++) {
			labelNames[i] = rsmd.getColumnLabel(i);
			types[i] = rsmd.getColumnType(i);
		}
	}
	
	//FIXME:copy
	private static byte[] handleBlob(Blob blob) throws SQLException {
		if (blob == null)
			return null;
		
		InputStream is = null;
		try {
			is = blob.getBinaryStream();
			byte[] data = new byte[(int)blob.length()];		// byte[] data = new byte[is.available()];
			is.read(data);
			is.close();
			return data;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			try {is.close();} catch (IOException e) {throw new RuntimeException(e);}
		}
	}
	
	//FIXME:copy
	private static String handleClob(Clob clob) throws SQLException {
		if (clob == null)
			return null;
		
		Reader reader = null;
		try {
			reader = clob.getCharacterStream();
			char[] buffer = new char[(int)clob.length()];
			reader.read(buffer);
			return new String(buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			try {reader.close();} catch (IOException e) {throw new RuntimeException(e);}
		}
	}
	
	//------------------
	
	public static void fillStatement(PreparedStatement pst, List<Object> paras) throws SQLException {
		for (int i=0, size=paras.size(); i<size; i++) {
			pst.setObject(i + 1, paras.get(i));
		}
	}
	
	public static void fillStatement(PreparedStatement pst, Object... paras) throws SQLException {
		for (int i=0; i<paras.length; i++) {
			pst.setObject(i + 1, paras[i]);
		}
	}
	
	public static void forPaginate(StringBuilder sql, int pageNumber, int pageSize, String select, String sqlExceptSelect) {
		int offset = pageSize * (pageNumber - 1);
		sql.append(select).append(" ");
		sql.append(sqlExceptSelect);
		sql.append(" limit ").append(offset).append(", ").append(pageSize);
	}
	
	public static void forModelSave(Table table, Map<String, Object> attrs, StringBuilder sql, List<Object> paras) {
		sql.append("insert into ").append(table.getName()).append("(");
		StringBuilder temp = new StringBuilder(") values(");
		for (Entry<String, Object> e: attrs.entrySet()) {
			String colName = e.getKey();
			if (table.hasColumnLabel(colName)) {
				if (paras.size() > 0) {
					sql.append(", ");
					temp.append(", ");
				}
				sql.append(colName);
				temp.append("?");
				paras.add(e.getValue());
			}
		}
		sql.append(temp.toString()).append(")");
	}
	
	public static String forModelFindById(Table table, String columns) {
		StringBuilder sql = new StringBuilder("select ");
		if (columns.trim().equals("*")) {
			sql.append(columns);
		}
		else {
			String[] columnsArray = columns.split(",");
			for (int i=0; i<columnsArray.length; i++) {
				if (i > 0)
					sql.append(", ");
				sql.append(columnsArray[i].trim());
			}
		}
		sql.append(" from ");
		sql.append(table.getName());
		sql.append(" where ").append(table.getPrimaryKey()).append(" = ?");
		return sql.toString();
	}
	
	public static void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, String pKey, Object id, StringBuilder sql, List<Object> paras) {
		sql.append("update ").append(table.getName()).append(" set ");
		for (Entry<String, Object> e : attrs.entrySet()) {
			String colName = e.getKey();
			if (!pKey.equalsIgnoreCase(colName) && modifyFlag.contains(colName) && table.hasColumnLabel(colName)) {
				if (paras.size() > 0)
					sql.append(", ");
				sql.append(colName).append(" = ? ");
				paras.add(e.getValue());
			}
		}
		sql.append(" where ").append(pKey).append(" = ?");
		paras.add(id);
	}
	
	public static String forModelDeleteById(Table table) {
		String pKey = table.getPrimaryKey();
		StringBuilder sql = new StringBuilder(45);
		sql.append("delete from ");
		sql.append(table.getName());
		sql.append(" where ").append(pKey).append(" = ?");
		return sql.toString();
	}
	
	public static String getDefaultPrimaryKey() {
		return "id";
	}
	
	public static void forDbUpdate(String tableName, String primaryKey, Object id, Record record, StringBuilder sql, List<Object> paras) {
		sql.append("update ").append(tableName.trim()).append(" set ");
		for (Entry<String, Object> e: record.getColumns().entrySet()) {
			String colName = e.getKey();
			if (!primaryKey.equalsIgnoreCase(colName)) {
				if (paras.size() > 0) {
					sql.append(", ");
				}
				sql.append(colName).append(" = ? ");
				paras.add(e.getValue());
			}
		}
		sql.append(" where ").append(primaryKey).append(" = ?");
		paras.add(id);
	}
	
	public static void forDbSave(StringBuilder sql, List<Object> paras, String tableName, Record record) {
		sql.append("insert into ");
		sql.append(tableName.trim()).append("(");
		StringBuilder temp = new StringBuilder();
		temp.append(") values(");
		
		for (Entry<String, Object> e: record.getColumns().entrySet()) {
			if (paras.size() > 0) {
				sql.append(", ");
				temp.append(", ");
			}
			sql.append(e.getKey());
			temp.append("?");
			paras.add(e.getValue());
		}
		sql.append(temp.toString()).append(")");
	}
	
	public static String forDbFindById(String tableName, String primaryKey, String columns) {
		StringBuilder sql = new StringBuilder("select ");
		if (columns.trim().equals("*")) {
			sql.append(columns);
		}
		else {
			String[] columnsArray = columns.split(",");
			for (int i=0; i<columnsArray.length; i++) {
				if (i > 0)
					sql.append(", ");
				sql.append(columnsArray[i].trim());
			}
		}
		sql.append(" from ");
		sql.append(tableName.trim());
		sql.append(" where ").append(primaryKey).append(" = ?");
		return sql.toString();
	}
	
	public static String forDbDeleteById(String tableName, String primaryKey) {
		StringBuilder sql = new StringBuilder("delete from ");
		sql.append(tableName.trim());
		sql.append(" where ").append(primaryKey).append(" = ?");
		return sql.toString();
	}
	
	public static String forTableBuilderDoBuild(String tableName) {
		return "select * from " + tableName + " where 1 = 2";
	}
}



