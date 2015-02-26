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
import java.io.Serializable;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * Model.
 * <p>
 * A clever person solves a problem.
 * A wise person avoids it.
 * A stupid person makes it.
 */
public abstract class Model<M extends Model<M>> implements Serializable {
	
	private static final long serialVersionUID = -990334519496260591L;
	
	/**
	 * Attributes of this model
	 */
	private Map<String, Object> attrs = getAttrsMap();	// getConfig().containerFactory.getAttrsMap();	// new HashMap<String, Object>();
	
	private Map<String, Object> getAttrsMap() {
		return new HashMap<String, Object>();
	}
	
	/**
	 * Flag of column has been modified. update need this flag
	 */
	private Set<String> modifyFlag;
	
	/*
	private Set<String> getModifyFlag() {
		if (modifyFlag == null)
			modifyFlag = getConfig().containerFactory.getModifyFlagSet();	// new HashSet<String>();
		return modifyFlag;
	}*/
	
	private Set<String> getModifyFlag() {
		if (modifyFlag == null) {
			modifyFlag = new HashSet<String>();
		}
		return modifyFlag;
	}
	
	private Config getConfig() {
		return DbPro.getConfig(getClass());
	}
	
	private Table getTable() {
		return DbPro.getTable(getClass());
	}
	
	/**
	 * Set attribute to model.
	 * @param attr the attribute name of the model
	 * @param value the value of the attribute
	 * @return this model
	 * @throws SQLException 
	 * @throws ActiveRecordException if the attribute is not exists of the model
	 */
	public M set(String attr, Object value) {
		if (getTable().hasColumnLabel(attr)) {
			attrs.put(attr, value);
			getModifyFlag().add(attr);	// Add modify flag, update() need this flag.
			return (M)this;
		}
		throw new IllegalArgumentException("The attribute name is not exists: " + attr);
	}
	
	/**
	 * Put key value pair to the model when the key is not attribute of the model.
	 */
	public M put(String key, Object value) {
		attrs.put(key, value);
		return (M)this;
	}
	
	/**
	 * Get attribute of any mysql type
	 */
	public <T> T get(String attr) {
		return (T)(attrs.get(attr));
	}
	
	/**
	 * Get attribute of any mysql type. Returns defaultValue if null.
	 */
	public <T> T get(String attr, Object defaultValue) {
		Object result = attrs.get(attr);
		return (T)(result != null ? result : defaultValue);
	}
	
	/**
	 * Get attribute of mysql type: varchar, char, enum, set, text, tinytext, mediumtext, longtext
	 */
	public String getStr(String attr) {
		return (String)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: int, integer, tinyint(n) n > 1, smallint, mediumint
	 */
	public Integer getInt(String attr) {
		return (Integer)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: bigint, unsign int
	 */
	public Long getLong(String attr) {
		return (Long)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: unsigned bigint
	 */
	public java.math.BigInteger getBigInteger(String attr) {
		return (java.math.BigInteger)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: date, year
	 */
	public java.util.Date getDate(String attr) {
		return (java.util.Date)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: time
	 */
	public java.sql.Time getTime(String attr) {
		return (java.sql.Time)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: timestamp, datetime
	 */
	public java.sql.Timestamp getTimestamp(String attr) {
		return (java.sql.Timestamp)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: real, double
	 */
	public Double getDouble(String attr) {
		return (Double)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: float
	 */
	public Float getFloat(String attr) {
		return (Float)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: bit, tinyint(1)
	 */
	public Boolean getBoolean(String attr) {
		return (Boolean)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: decimal, numeric
	 */
	public java.math.BigDecimal getBigDecimal(String attr) {
		return (java.math.BigDecimal)attrs.get(attr);
	}
	
	/**
	 * Get attribute of mysql type: binary, varbinary, tinyblob, blob, mediumblob, longblob
	 */
	public byte[] getBytes(String attr) {
		return (byte[])attrs.get(attr);
	}
	
	/**
	 * Get attribute of any type that extends from Number
	 */
	public Number getNumber(String attr) {
		return (Number)attrs.get(attr);
	}
	
	/**
	 * Paginate.
	 * @param pageNumber the page number
	 * @param pageSize the page size
	 * @param select the select part of the sql statement 
	 * @param sqlExceptSelect the sql statement excluded select part
	 * @param paras the parameters of sql
	 * @return Page
	 * @throws SQLException 
	 */
	public Page<M> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
		Config config = getConfig();
		Connection conn = null;
		try {
			conn = config.getConnection();
			return paginate(config, conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
		} finally {
			config.close(conn);
		}
	}
	
	private Page<M> paginate(Config config, Connection conn, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
		if (pageNumber < 1 || pageSize < 1) {
			throw new SQLException("pageNumber and pageSize must be more than 0");
		}
//		if (config.dialect.isTakeOverModelPaginate()) {
//			return config.dialect.takeOverModelPaginate(conn, getClass(), pageNumber, pageSize, select, sqlExceptSelect, paras);
//		}
		long totalRow = 0;
		int totalPage = 0;
		
		//FIXME: begin
		List result = new ArrayList();
		{
			String sql = "select count(*) " + replaceFormatSqlOrderBy(sqlExceptSelect);
			PreparedStatement pst = conn.prepareStatement(sql);
			DbPro.fillStatement(pst, paras);
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
		}
		//FIXME: end
		
		int size = result.size();
		if (size == 1)
			totalRow = ((Number)result.get(0)).longValue();		// totalRow = (Long)result.get(0);
		else if (size > 1)
			totalRow = result.size();
		else
			return new Page<M>(new ArrayList<M>(0), pageNumber, pageSize, 0, 0);	// totalRow = 0;
		
		totalPage = (int) (totalRow / pageSize);
		if (totalRow % pageSize != 0) {
			totalPage++;
		}
		
		// --------
		StringBuilder sql = new StringBuilder();
		DbPro.forPaginate(sql, pageNumber, pageSize, select, sqlExceptSelect);
		List<M> list = find(conn, sql.toString(), paras);
		return new Page<M>(list, pageNumber, pageSize, totalPage, (int)totalRow);
	}
	
	/**
	 * @throws SQLException 
	 * @see #paginate(int, int, String, String, Object...)
	 */
	public Page<M> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect) throws SQLException {
		return paginate(pageNumber, pageSize, select, sqlExceptSelect, DbPro.NULL_PARA_ARRAY);
	}
	
	/**
	 * Return attribute Map.
	 * <p>
	 * Danger! The update method will ignore the attribute if you change it directly.
	 * You must use set method to change attribute that update method can handle it.
	 */
	public Map<String, Object> getAttrs() {
		return attrs;
	}
	
	/**
	 * Return attribute Set.
	 */
	public Set<Entry<String, Object>> getAttrsEntrySet() {
		return attrs.entrySet();
	}
	
	/**
	 * Save model.
	 * @throws SQLException 
	 */
	public boolean save() throws SQLException {
		Config config = getConfig();
		Table table = getTable();
		
		StringBuilder sql = new StringBuilder();
		List<Object> paras = new ArrayList<Object>();
		DbPro.forModelSave(table, attrs, sql, paras);
		// if (paras.size() == 0)	return false;	// The sql "insert into tableName() values()" works fine, so delete this line
		
		// --------
		Connection conn = null;
		PreparedStatement pst = null;
		int result = 0;
		try {
			conn = config.getConnection();
			pst = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			DbPro.fillStatement(pst, paras);
			result = pst.executeUpdate();
			getGeneratedKey(pst, table);
			getModifyFlag().clear();
			return result >= 1;
		} finally {
			config.close(pst, conn);
		}
	}
	
	/**
	 * Get id after save method.
	 */
	private void getGeneratedKey(PreparedStatement pst, Table table) throws SQLException {
		String pKey = table.getPrimaryKey();
		if (get(pKey) == null) {
			ResultSet rs = pst.getGeneratedKeys();
			if (rs.next()) {
				Class<?> colType = table.getColumnType(pKey);
				if (colType == Integer.class || colType == int.class)
					set(pKey, rs.getInt(1));
				else if (colType == Long.class || colType == long.class)
					set(pKey, rs.getLong(1));
				else
					set(pKey, rs.getObject(1));		// It returns Long object for int colType
				rs.close();
			}
		}
	}
	
	/**
	 * Delete model.
	 * @throws SQLException 
	 */
	public boolean delete() throws SQLException {
		Table table = getTable();
		Object id = attrs.get(table.getPrimaryKey());
		if (id == null) {
			throw new SQLException("You can't delete model without id.");
		}
		return deleteById(table, id);
	}
	
	/**
	 * Delete model by id.
	 * @param id the id value of the model
	 * @return true if delete succeed otherwise false
	 * @throws SQLException 
	 */
	public boolean deleteById(Object id) throws SQLException {
		if (id == null) {
			throw new IllegalArgumentException("id can not be null");
		}
		return deleteById(getTable(), id);
	}
	
	private boolean deleteById(Table table, Object id) throws SQLException {
		Config config = getConfig();
		Connection conn = null;
		try {
			conn = config.getConnection();
			String sql = DbPro.forModelDeleteById(table);
			
			//FIXME:
			PreparedStatement pst = conn.prepareStatement(sql);
			DbPro.fillStatement(pst, id);
			int result = pst.executeUpdate();
			closeQuietly(pst);
			
			return result >= 1;
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * Update model.
	 * @throws SQLException 
	 */
	public boolean update() throws SQLException {
		if (getModifyFlag().isEmpty())
			return false;
		
		Table table = getTable();
		String pKey = table.getPrimaryKey();
		Object id = attrs.get(pKey);
		if (id == null) {
			throw new SQLException("You can't update model without Primary Key.");
		}
		Config config = getConfig();
		StringBuilder sql = new StringBuilder();
		List<Object> paras = new ArrayList<Object>();
		DbPro.forModelUpdate(table, attrs, getModifyFlag(), pKey, id, sql, paras);
		
		if (paras.size() <= 1) {	// Needn't update
			return false;
		}
		
		// --------
		Connection conn = null;
		try {
			conn = config.getConnection();
			
			//FIXME:
			PreparedStatement pst = conn.prepareStatement(sql.toString());
			DbPro.fillStatement(pst, paras.toArray());
			int result = pst.executeUpdate();
			closeQuietly(pst);
			//FIXME:
			
			if (result >= 1) {
				getModifyFlag().clear();
				return true;
			}
			return false;
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * Find model.
	 * @throws SQLException 
	 */
	private List<M> find(Connection conn, String sql, Object... paras) throws SQLException {
		Config config = getConfig();
		Class<? extends Model> modelClass = getClass();
		if (config.devMode)
			checkTableName(modelClass, sql);
		
		PreparedStatement pst = conn.prepareStatement(sql);
		DbPro.fillStatement(pst, paras);
		ResultSet rs = pst.executeQuery();
		List<M> result = build(rs, modelClass);
		closeQuietly(rs, pst);
		return result;
	}
	
	/**
	 * Find model.
	 * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param paras the parameters of sql
	 * @return the list of Model
	 * @throws SQLException 
	 */
	public List<M> find(String sql, Object... paras) throws SQLException {
		Config config = getConfig();
		Connection conn = null;
		try {
			conn = config.getConnection();
			return find(conn, sql, paras);
		} finally {
			config.close(conn);
		}
	}
	
	/**
	 * Check the table name. The table name must in sql.
	 */
	private void checkTableName(Class<? extends Model> modelClass, String sql) {
		Table table = DbPro.getTable(modelClass);
		if (! sql.toLowerCase().contains(table.getName().toLowerCase())) {
			throw new IllegalArgumentException("The table name: " + table.getName() + " not in your sql.");
		}
	}
	
	/**
	 * @throws SQLException 
	 * @see #find(String, Object...)
	 */
	public List<M> find(String sql) throws SQLException {
		return find(sql, DbPro.NULL_PARA_ARRAY);
	}
	
	/**
	 * Find first model. I recommend add "limit 1" in your sql.
	 * @param sql an SQL statement that may contain one or more '?' IN parameter placeholders
	 * @param paras the parameters of sql
	 * @return Model
	 * @throws SQLException 
	 */
	public M findFirst(String sql, Object... paras) throws SQLException {
		List<M> result = find(sql, paras);
		return result.size() > 0 ? result.get(0) : null;
	}
	
	/**
	 * @see #findFirst(String, Object...)
	 * @param sql an SQL statement
	 * @throws SQLException 
	 */
	public M findFirst(String sql) throws SQLException {
		List<M> result = find(sql, DbPro.NULL_PARA_ARRAY);
		return result.size() > 0 ? result.get(0) : null;
	}
	
	/**
	 * Find model by id.
	 * @param id the id value of the model
	 * @throws SQLException 
	 */
	public M findById(Object id) throws SQLException {
		return findById(id, "*");
	}
	
	/**
	 * Find model by id. Fetch the specific columns only.
	 * Example: User user = User.dao.findById(15, "name, age");
	 * @param id the id value of the model
	 * @param columns the specific columns separate with comma character ==> ","
	 * @throws SQLException 
	 */
	public M findById(Object id, String columns) throws SQLException {
		Table table = getTable();
		String sql = DbPro.forModelFindById(table, columns);
		List<M> result = find(sql, id);
		return result.size() > 0 ? result.get(0) : null;
	}
	
	/**
	 * Set attributes with other model.
	 * @param model the Model
	 * @return this Model
	 * @throws SQLException 
	 */
	public M setAttrs(M model) throws SQLException {
		return setAttrs(model.getAttrs());
	}
	
	/**
	 * Set attributes with Map.
	 * @param attrs attributes of this model
	 * @return this Model
	 * @throws SQLException 
	 */
	public M setAttrs(Map<String, Object> attrs) throws SQLException {
		for (Entry<String, Object> e : attrs.entrySet()) {
			set(e.getKey(), e.getValue());
		}
		return (M)this;
	}
	
	/**
	 * Remove attribute of this model.
	 * @param attr the attribute name of the model
	 * @return this model
	 */
	public M remove(String attr) {
		attrs.remove(attr);
		getModifyFlag().remove(attr);
		return (M)this;
	}
	
	/**
	 * Remove attributes of this model.
	 * @param attrs the attribute names of the model
	 * @return this model
	 */
	public M remove(String... attrs) {
		if (attrs != null)
			for (String a : attrs) {
				this.attrs.remove(a);
				this.getModifyFlag().remove(a);
			}
		return (M)this;
	}
	
	/**
	 * Remove attributes if it is null.
	 * @return this model
	 */
	public M removeNullValueAttrs() {
		for (Iterator<Entry<String, Object>> it = attrs.entrySet().iterator(); it.hasNext();) {
			Entry<String, Object> e = it.next();
			if (e.getValue() == null) {
				it.remove();
				getModifyFlag().remove(e.getKey());
			}
		}
		return (M)this;
	}
	
	/**
	 * Keep attributes of this model and remove other attributes.
	 * @param attrs the attribute names of the model
	 * @return this model
	 */
	public M keep(String... attrs) {
		if (attrs != null && attrs.length > 0) {
			Map<String, Object> newAttrs = new HashMap<String, Object>();
			Set<String> newModifyFlag = new HashSet<String>();
			for (String a : attrs) {
				if (this.attrs.containsKey(a))	// prevent put null value to the newColumns
					newAttrs.put(a, this.attrs.get(a));
				if (this.getModifyFlag().contains(a))
					newModifyFlag.add(a);
			}
			this.attrs = newAttrs;
			this.modifyFlag = newModifyFlag;
		}
		else {
			this.attrs.clear();
			this.getModifyFlag().clear();
		}
		return (M)this;
	}
	
	/**
	 * Keep attribute of this model and remove other attributes.
	 * @param attr the attribute name of the model
	 * @return this model
	 */
	public M keep(String attr) {
		if (attrs.containsKey(attr)) {	// prevent put null value to the newColumns
			Object keepIt = attrs.get(attr);
			boolean keepFlag = getModifyFlag().contains(attr);
			attrs.clear();
			getModifyFlag().clear();
			attrs.put(attr, keepIt);
			if (keepFlag)
				getModifyFlag().add(attr);
		}
		else {
			attrs.clear();
			getModifyFlag().clear();
		}
		return (M)this;
	}
	
	/**
	 * Remove all attributes of this model.
	 * @return this model
	 */
	public M clear() {
		attrs.clear();
		getModifyFlag().clear();
		return (M)this;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString()).append(" {");
		boolean first = true;
		for (Entry<String, Object> e : attrs.entrySet()) {
			if (first)
				first = false;
			else
				sb.append(", ");
			
			Object value = e.getValue();
			if (value != null)
				value = value.toString();
			sb.append(e.getKey()).append(":").append(value);
		}
		sb.append("}");
		return sb.toString();
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof Model))
            return false;
		if (o == this)
			return true;
		return this.attrs.equals(((Model)o).attrs);
	}
	
	public int hashCode() {
		return (attrs == null ? 0 : attrs.hashCode()) ^ (getModifyFlag() == null ? 0 : getModifyFlag().hashCode());
	}
	
	/**
	 * Return attribute names of this model.
	 */
	public String[] getAttrNames() {
		Set<String> attrNameSet = attrs.keySet();
		return attrNameSet.toArray(new String[attrNameSet.size()]);
	}
	
	/**
	 * Return attribute values of this model.
	 */
	public Object[] getAttrValues() {
		java.util.Collection<Object> attrValueCollection = attrs.values();
		return attrValueCollection.toArray(new Object[attrValueCollection.size()]);
	}
	
	/**
	 * Return json string of this model.
	 */
	public String toJson() {
		return com.iteye.weimingtom.sharuru.JsonKit.toJson(attrs, 4);
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
	
	
	public static final <T> List<T> build(ResultSet rs, Class<? extends Model> modelClass) throws SQLException {
		List<T> result = new ArrayList<T>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		String[] labelNames = new String[columnCount + 1];
		int[] types = new int[columnCount + 1];
		buildLabelNamesAndTypes(rsmd, labelNames, types);
		while (rs.next()) {
			Model<?> ar;
			try {
				ar = modelClass.newInstance();
				Map<String, Object> attrs = ar.getAttrs();
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
					
					attrs.put(labelNames[i], value);
				}
				result.add((T)ar);
			} catch (InstantiationException e) {
				throw new IllegalArgumentException(e);
			} catch (IllegalAccessException e) {
				throw new IllegalArgumentException(e);
			}
		}
		return result;
	}
	
	private static final void buildLabelNamesAndTypes(ResultSetMetaData rsmd, String[] labelNames, int[] types) throws SQLException {
		for (int i=1; i<labelNames.length; i++) {
			labelNames[i] = rsmd.getColumnLabel(i);
			types[i] = rsmd.getColumnType(i);
		}
	}
	
	public static byte[] handleBlob(Blob blob) throws SQLException {
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
	
	public static String handleClob(Clob clob) throws SQLException {
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
}


