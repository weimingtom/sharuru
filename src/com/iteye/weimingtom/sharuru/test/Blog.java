package com.iteye.weimingtom.sharuru.test;


import java.sql.SQLException;

import com.iteye.weimingtom.sharuru.Model;
import com.iteye.weimingtom.sharuru.Page;

/**
 * Blog model.

将表结构放在此，消除记忆负担
mysql> desc blog;
+---------+--------------+------+-----+---------+----------------+
| Field   | Type         | Null | Key | Default | Extra          |
+---------+--------------+------+-----+---------+----------------+
| id      | int(11)      | NO   | PRI | NULL    | auto_increment |
| title   | varchar(200) | NO   |     | NULL    |                |
| content | mediumtext   | NO   |     | NULL    |                |
+---------+--------------+------+-----+---------+----------------+

数据库字段名建议使用驼峰命名规则，便于与 java 代码保持�?��，如字段名： userId
 */
@SuppressWarnings("serial")
public class Blog extends Model<Blog> {
	public static final Blog me = new Blog();
	
	/**
	 * �?�� sql 与业务�?辑写�?Model �?Service 中，不要写在 Controller 中，养成好习惯，有利于大型项目的�?��与维�?
	 * @throws SQLException 
	 */
	public Page<Blog> paginate(int pageNumber, int pageSize) throws SQLException {
		return paginate(pageNumber, pageSize, "select *", "from blog order by id asc");
	}
}
