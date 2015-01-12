package com.anjuke.hive.storage.db;

import org.apache.hadoop.conf.Configuration;

public class DaoFactory {
    
    public static Dao getDao(Configuration conf) {
        Dao dao = new MySQLDao();
        dao.setConnnection(ConnectionManager.getInstance(conf).getConnection());
        
        return dao;
    }

}
