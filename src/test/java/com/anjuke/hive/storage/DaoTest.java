package com.anjuke.hive.storage;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.anjuke.hive.storage.db.Dao;
import com.anjuke.hive.storage.db.DaoFactory;
import com.anjuke.hive.storage.jdbc.HiveConfiguration;

public class DaoTest {
    
    private Dao dao;
    
    @Before
    public void initDao() {
        Configuration conf = new Configuration();
        conf.set(HiveConfiguration.JDBC_DRIVER_CLASS, "com.mysql.jdbc.Driver");
        conf.set(HiveConfiguration.JDBC_URL, "jdbc:mysql://localhost:3306/t_db?user=root&password=123456&characterEncoding=utf8");
        
        dao = DaoFactory.getDao(conf);
    }
    
    @Test
    public void testDao() {
        assertNotNull(dao);
    }
    
    @Test
    public void testGetRowDataLength() {
        dao.setTableName("q1");
        dao.setSelectFields(Arrays.asList(new String[]{"id"}));
        
        int rowlen = dao.getRowDataLength();
        
        assertTrue(rowlen > 0);
        assertTrue(rowlen == 4);
        
        dao.setTableName("place_info");
        dao.setSelectFields(Arrays.asList(new String[]{"id", "lng", "lat", "geocode", "place_info_id", "type"}));
        
        rowlen = dao.getRowDataLength();
        assertTrue(rowlen > 0);
    }
    
    @Test
    public void testGetAffetcedRows() {
        dao.setTableName("place_info");
        dao.setSelectFields(Arrays.asList(new String[]{"id", "lng", "lat", "geocode", "place_info_id", "type"}));
        
        long affectedRows = dao.getAffetcedRows();
        assertTrue(affectedRows > 0);
    }

}
