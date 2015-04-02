package com.anjuke.hive.storage.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcRecordIterator {
    
    private Connection conn;
    
    private PreparedStatement ps;
    
    private ResultSet rs;
    
    private ResultSetMetaData metadata;

    private int numColumns;
    
    private static final Logger LOG = LoggerFactory.getLogger(JdbcRecordIterator.class);
    
    public JdbcRecordIterator(Connection conn, PreparedStatement ps, ResultSet rs) {
        this.conn = conn;
        this.ps = ps;
        this.rs = rs;
        
        try {
            this.metadata = rs.getMetaData();
            this.numColumns = metadata.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean hasNext() {
        try {
            return rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Map<String, String> next() {
        try {
            Map<String, String> record = new HashMap<String, String>(numColumns);
            for (int i = 1; i <= numColumns; i++) {
                String key = metadata.getColumnName(i);
                String value = rs.getString(i);
                /*if (value == null) {
                    value = NullWritable.get().toString();
                }*/
                
                record.put(key, value);
            }

            return record;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return null;
        }
    }
    
    public void close() {
        try {
            rs.close();
            ps.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
