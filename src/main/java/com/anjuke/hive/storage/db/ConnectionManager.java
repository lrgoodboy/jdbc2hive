package com.anjuke.hive.storage.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;

import com.anjuke.hive.storage.jdbc.HiveConfiguration;

public class ConnectionManager {
    
    private Configuration conf;
    
    private DataSource dbcpDataSource = null;
    
    private static HashMap<Configuration, ConnectionManager> instances 
        = new HashMap<Configuration, ConnectionManager>();
    
    public static ConnectionManager getInstance(Configuration conf) {
        if (instances.get(conf) == null) {
            instances.put(conf, new ConnectionManager(conf));
        }
        
        return instances.get(conf); 
    }
    
    private ConnectionManager(Configuration conf) {
        this.conf = conf;
        
        try {
            dbcpDataSource = BasicDataSourceFactory.createDataSource(getDBProps());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private Properties getDBProps() {
        Properties props = new Properties();
        props.put("initialSize", "1");
        props.put("maxActive", "5");
        props.put("maxIdle", "1");
        props.put("maxWait", "10000");
        props.put("timeBetweenEvictionRunsMillis", "30000");
        
        // override with user defined properties
        Map<String, String> userProperties = conf.getValByRegex(HiveConfiguration.DBCP_CONFIG_PREFIX + "\\.*");
        if ((userProperties != null) && (!userProperties.isEmpty())) {
            for (Entry<String, String> entry : userProperties.entrySet()) {
                props.put(entry.getKey().replaceFirst(HiveConfiguration.DBCP_CONFIG_PREFIX + "\\.", ""), entry.getValue());
            }
        }

        // essential properties that shouldn't be overridden by users
        props.put("url", conf.get(HiveConfiguration.JDBC_URL));
        props.put("driverClassName", conf.get(HiveConfiguration.JDBC_DRIVER_CLASS));
        props.put("type", "javax.sql.DataSource");
        
        return props;
    }
    
    public Connection getConnection() {
        try {
            return dbcpDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

}
