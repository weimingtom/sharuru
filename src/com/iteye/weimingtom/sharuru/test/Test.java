package com.iteye.weimingtom.sharuru.test;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.iteye.weimingtom.sharuru.Config;
import com.iteye.weimingtom.sharuru.DbPro;
import com.iteye.weimingtom.sharuru.Page;
import com.iteye.weimingtom.sharuru.Record;

public class Test {
	public static void main(String[] args) {
        Config cfg = new Config();
        cfg.setupActiveRecordPlugin("jdbc:sqlite:jfinal_demo.db");
        cfg.setTransactionLevel(Connection.TRANSACTION_SERIALIZABLE);
        cfg.addMapping("blog", Blog.class);
        cfg.setDevMode(true);
        cfg.setShowSql(true);
        try {
			DbPro.startActiveRecordPlugin(cfg);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
        
        try {
	        List<Blog> blogs = Blog.me.find("select * from blog order by id asc");
	        for(Blog d:blogs){
	            Set<Entry<String, Object>> set = d.getAttrsEntrySet();
	            Iterator<Entry<String, Object>> it = set.iterator();
	            System.out.println("=================");
	            while(it.hasNext()){
	                Entry<String, Object> next = it.next();
	                System.out.println(next.getKey()+":"+next.getValue());
	            }
	            System.out.println("=================");
	        }
//	        for(Blog d:blogs) {
//        	System.out.println("==>" + d.toJson());
//        }
        } catch (SQLException e) {
			e.printStackTrace();
		}
        
        try {
	        DbPro.use().tx(new Runnable() {
	        	public void run() {
	        		try {
	        			DbPro.use().update("update blog set title = ? where id = ?", "test 1", 2);
		        		DbPro.use().update("update blog set title = ? where id = ?", "test 2", 3);
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
	        	}
	        });
        } catch (Throwable e) {
        	e.printStackTrace();
        }
        
        try {
        	List<Record> blogs2 = DbPro.use().find("select * from blog order by id asc");
			for(Record record:blogs2) {
	        	System.out.println("==>" + record.toJson());
	        }
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
        try {
	        Page<Record> blogs3 = DbPro.use().paginate(2, 2, "select *","from blog");
	        for(Record record:blogs3.getList()) {
	        	System.out.println("paginate==>" + record.toJson());
	        }
        } catch (SQLException e) {
			e.printStackTrace();
		}
        
		DbPro.stopActiveRecordPlugin(cfg);
    }
}
