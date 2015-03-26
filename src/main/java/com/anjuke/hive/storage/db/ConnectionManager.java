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
    
    private DataSource dbcpDataSource = null;
    
    private static HashMap<String, ConnectionManager> instances 
        = new HashMap<String, ConnectionManager>();
    
    public static ConnectionManager getInstance(Configuration conf) {
        Properties props = getDBProps(conf);
        String propsStr = props.toString();
        
        if (instances.get(propsStr) == null) {
            instances.put(propsStr, new ConnectionManager(props));
        }
        
        return instances.get(propsStr); 
    }
    
    private ConnectionManager(Properties props) {
        try {
            dbcpDataSource = BasicDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static Properties getDBProps(Configuration conf) {
        Properties props = new Properties();
        props.put("initialSize", "1");
        props.put("maxActive", "-1");
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
