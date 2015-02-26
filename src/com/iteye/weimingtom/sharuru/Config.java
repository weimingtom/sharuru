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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import org.sqlite.SQLiteDataSource;


public class Config {
	boolean isStarted = false;
	List<Table> tableList = new ArrayList<Table>();
	
	public void setupActiveRecordPlugin(String jdbcUrl) {
		String configName = DbPro.MAIN_CONFIG_NAME;
		this.name = configName.trim();
		SQLiteDataSource dataSource = new SQLiteDataSource();
		dataSource.setUrl(jdbcUrl);
		this.dataSource = dataSource;
		this.setTransactionLevel(Connection.TRANSACTION_SERIALIZABLE);
	}
	
	public Config addMapping(String tableName, String primaryKey, Class<? extends Model<?>> modelClass) {
		tableList.add(new Table(tableName, primaryKey, modelClass));
		return this;
	}
	
	public Config addMapping(String tableName, Class<? extends Model<?>> modelClass) {
		tableList.add(new Table(tableName, modelClass));
		return this;
	}
	
	/**
	 * Set transaction level define in java.sql.Connection
	 * @param transactionLevel only be 0, 1, 2, 4, 8
	 */
	public Config setTransactionLevel(int transactionLevel) {
		int t = transactionLevel;
		if (t != 0 && t != 1  && t != 2  && t != 4  && t != 8)
			throw new IllegalArgumentException("The transactionLevel only be 0, 1, 2, 4, 8");
		this.transactionLevel = transactionLevel;
		return this;
	}
	
	public Config setShowSql(boolean showSql) {
		this.showSql = showSql;
		return this;
	}
	
	public Config setDevMode(boolean devMode) {
		this.devMode = devMode;
		return this;
	}
	
	boolean build() throws SQLException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			for (Table table : tableList) {
				doBuild(table, conn);
				DbPro.putTable(table);
				DbPro.addModelToConfigMapping(table.getModelClass(), this);
			}
			return true;
		} finally {
			close(conn);
		}
	}
	
	private void doBuild(Table table, Connection conn) throws SQLException {
		table.setColumnTypeMap(new HashMap<String, Class<?>>());
		if (table.getPrimaryKey() == null)
			table.setPrimaryKey(DbPro.getDefaultPrimaryKey());
		
		String sql = DbPro.forTableBuilderDoBuild(table.getName());
		Statement stm = conn.createStatement();
		ResultSet rs = stm.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		
		for (int i=1; i<=rsmd.getColumnCount(); i++) {
			String colName = rsmd.getColumnName(i);
			String colClassName = rsmd.getColumnClassName(i);
			if ("java.lang.String".equals(colClassName)) {
				// varchar, char, enum, set, text, tinytext, mediumtext, longtext
				table.setColumnType(colName, java.lang.String.class);
			}
			else if ("java.lang.Integer".equals(colClassName)) {
				// int, integer, tinyint, smallint, mediumint
				table.setColumnType(colName, java.lang.Integer.class);
			}
			else if ("java.lang.Long".equals(colClassName)) {
				// bigint
				table.setColumnType(colName, java.lang.Long.class);
			}
			// else if ("java.util.Date".equals(colClassName)) {		// java.util.Data can not be returned
				// java.sql.Date, java.sql.Time, java.sql.Timestamp all extends java.util.Data so getDate can return the three types data
				// result.addInfo(colName, java.util.Date.class);
			// }
			else if ("java.sql.Date".equals(colClassName)) {
				// date, year
				table.setColumnType(colName, java.sql.Date.class);
			}
			else if ("java.lang.Double".equals(colClassName)) {
				// real, double
				table.setColumnType(colName, java.lang.Double.class);
			}
			else if ("java.lang.Float".equals(colClassName)) {
				// float
				table.setColumnType(colName, java.lang.Float.class);
			}
			else if ("java.lang.Boolean".equals(colClassName)) {
				// bit
				table.setColumnType(colName, java.lang.Boolean.class);
			}
			else if ("java.sql.Time".equals(colClassName)) {
				// time
				table.setColumnType(colName, java.sql.Time.class);
			}
			else if ("java.sql.Timestamp".equals(colClassName)) {
				// timestamp, datetime
				table.setColumnType(colName, java.sql.Timestamp.class);
			}
			else if ("java.math.BigDecimal".equals(colClassName)) {
				// decimal, numeric
				table.setColumnType(colName, java.math.BigDecimal.class);
			}
			else if ("[B".equals(colClassName)) {
				// binary, varbinary, tinyblob, blob, mediumblob, longblob
				// qjd project: print_info.content varbinary(61800);
				table.setColumnType(colName, byte[].class);
			}
			else {
				int type = rsmd.getColumnType(i);
				if (type == Types.BLOB) {
					table.setColumnType(colName, byte[].class);
				}
				else if (type == Types.CLOB || type == Types.NCLOB) {
					table.setColumnType(colName, String.class);
				}
				else {
					table.setColumnType(colName, String.class);
				}
				// core.TypeConverter
				// throw new RuntimeException("You've got new type to mapping. Please add code in " + TableBuilder.class.getName() + ". The ColumnClassName can't be mapped: " + colClassName);
			}
		}
		
		rs.close();
		stm.close();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//-------------------------------------
	//Config
	
	String name = DbPro.MAIN_CONFIG_NAME;
	
	private final ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();
	
	public DataSource dataSource;
	int transactionLevel = Connection.TRANSACTION_READ_COMMITTED;
	
	boolean showSql = false;
	boolean devMode = false;

	/**
	 * For DbKit.brokenConfig = new Config();
	 */
	public Config() {
		
	}
	
	/**
	 * Constructor with DataSource
	 * @param dataSource the dataSource, can not be null
	 */
	public Config(String name, DataSource dataSource) {
		if (isBlank(name))
			throw new IllegalArgumentException("Config name can not be blank");
		if (dataSource == null)
			throw new IllegalArgumentException("DataSource can not be null");
		
		this.name = name.trim();
		this.dataSource = dataSource;
	}
	
	/**
	 * Constructor with full parameters
	 * @param dataSource the dataSource, can not be null
	 * @param dialect the dialect, set null with default value: new MysqlDialect()
	 * @param showSql the showSql,set null with default value: false
	 * @param devMode the devMode, set null with default value: false
	 * @param transactionLevel the transaction level, set null with default value: Connection.TRANSACTION_READ_COMMITTED
	 * @param containerFactory the containerFactory, set null with default value: new IContainerFactory(){......}
	 * @param cache the cache, set null with default value: new EhCache()
	 */
	public Config(String name,
				  DataSource dataSource,
				  Boolean showSql,
				  Boolean devMode,
				  Integer transactionLevel) {
		if (isBlank(name))
			throw new IllegalArgumentException("Config name can not be blank");
		if (dataSource == null)
			throw new IllegalArgumentException("DataSource can not be null");
		
		this.name = name.trim();
		this.dataSource = dataSource;
		
		if (showSql != null)
			this.showSql = showSql;
		if (devMode != null)
			this.devMode = devMode;
		if (transactionLevel != null)
			this.transactionLevel = transactionLevel;
	}
	
	public String getName() {
		return name;
	}
	
	public int getTransactionLevel() {
		return transactionLevel;
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	public boolean isShowSql() {
		return showSql;
	}
	
	public boolean isDevMode() {
		return devMode;
	}
	
	// --------
	
	/**
	 * Support transaction with Transaction interceptor
	 */
	public final void setThreadLocalConnection(Connection connection) {
		threadLocal.set(connection);
	}
	
	public final void removeThreadLocalConnection() {
		threadLocal.remove();
	}
	
	/**
	 * Get Connection. Support transaction if Connection in ThreadLocal
	 */
	public final Connection getConnection() throws SQLException {
		Connection conn = threadLocal.get();
		if (conn != null)
			return conn;
		return showSql ? new SqlReporter(dataSource.getConnection()).getConnection() : dataSource.getConnection();
	}
	
	/**
	 * Helps to implement nested transaction.
	 * Tx.intercept(...) and Db.tx(...) need this method to detected if it in nested transaction.
	 */
	public final Connection getThreadLocalConnection() {
		return threadLocal.get();
	}
	
	/**
	 * Close ResultSet、Statement、Connection
	 * ThreadLocal support declare transaction.
	 */
	public final void close(ResultSet rs, Statement st, Connection conn) {
		if (rs != null) {try {rs.close();} catch (SQLException e) {}}
		if (st != null) {try {st.close();} catch (SQLException e) {}}
		
		if (threadLocal.get() == null) {	// in transaction if conn in threadlocal
			if (conn != null) {try {conn.close();}
			catch (SQLException e) {throw new IllegalArgumentException(e);}}
		}
	}
	
	public final void close(Statement st, Connection conn) {
		if (st != null) {try {st.close();} catch (SQLException e) {}}
		
		if (threadLocal.get() == null) {	// in transaction if conn in threadlocal
			if (conn != null) {try {conn.close();}
			catch (SQLException e) {throw new IllegalArgumentException(e);}}
		}
	}
	
	public final void close(Connection conn) {
		if (threadLocal.get() == null)		// in transaction if conn in threadlocal
			if (conn != null)
				try {conn.close();} catch (SQLException e) {throw new IllegalArgumentException(e);}
	}
	
	private static boolean isBlank(String str) {
		return str == null || "".equals(str.trim()) ? true : false;
	}
}



