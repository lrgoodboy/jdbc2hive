package com.anjuke.hive.storage.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;

public class HiveConfiguration {
    
    public final static String TABLENAME = "jdbc2hive.table.name";
    public final static String SPLITEDBY = "jdbc2hive.splited.by";
    public static final String JDBC_URL = "jdbc2hive.jdbc.url";
    public static final String JDBC_DRIVER_CLASS = "jdbc2hive.jdbc.class";
    public static final String DBCP_CONFIG_PREFIX = "jdbc2hive.dhcp";
    public static final String COLUMN_MAP = "jdbc2hive.column.map";
    
    public static final Set<String> REQUIRED_CONF = new HashSet<String>();
    static {
        REQUIRED_CONF.add(TABLENAME);
        REQUIRED_CONF.add(SPLITEDBY);
        REQUIRED_CONF.add(JDBC_URL);
        REQUIRED_CONF.add(JDBC_DRIVER_CLASS);
        
    }
    
    private Configuration conf;
    
    private static Map<Configuration, HiveConfiguration> instances = new HashMap<Configuration, HiveConfiguration>();
    
    public static HiveConfiguration getInstance(Configuration conf) {
        if (instances.get(conf) == null) {
            instances.put(conf, new HiveConfiguration(conf));
        }
        
        return instances.get(conf);
    }
    
    private HiveConfiguration(Configuration conf) {
        this.conf = conf;
    }
    
    public List<String> getDBSelectFields(List<String> hiveColumns) {
        Map<String, String> columnMap = getColumnMap();
        if (columnMap == null || columnMap.isEmpty()) {
            return hiveColumns;
        }
        
        List<String> dbFields = new ArrayList<String>(hiveColumns.size());
        for (String field : hiveColumns) {
            if(columnMap.get(field) != null) {
                field = columnMap.get(field);
            }
            
            dbFields.add(field);
        }
        
        return dbFields;
    }
    
    public String getSplitedBy() {
        return conf.get(SPLITEDBY);
    }
    
    public long getBlockSize() {
        return conf.getLong("dfs.blocksize", 67108864);
    }
    
    public ExprNodeDesc getExpNodeDesc () {
        String filterXml = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        return Utilities.deserializeExpression(filterXml, conf);
    }
    
    public Map<String, String> getColumnMap() {
        String columnMapStr = conf.get(COLUMN_MAP);
        if (Util.isEmpty(columnMapStr)) {
            return null;
        }
        
        String[] _tmps = columnMapStr.split(",");
        
        Map<String, String> map = new HashMap<String, String>(_tmps.length);
        for (String _tmp : _tmps) {
            int pos = _tmp.indexOf('=');
            if (pos == -1) {
                continue;
            }
            
            map.put(_tmp.substring(0, pos), _tmp.substring(pos+1));
        }
        
        return map;
    }
    
    public List<String> getHiveSelectedColumns() {
        // current hive query selected column id.
        String columnsStr = conf.get(org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
        
        // all hive columns and column types.
        String hiveColumns = conf.get(serdeConstants.LIST_COLUMNS);
        String hiveColumnsTypeStr = conf.get(serdeConstants.LIST_COLUMN_TYPES);
        
        
        if (columnsStr != null && !columnsStr.trim().isEmpty()) {
            String[] hiveColumnsArray = hiveColumns.split(",");
            String[] columns = columnsStr.split(",");
            
            List<String> selectedColumns = new ArrayList<String>(columns.length);
            
            for (String _columnStr : columns) {
                int _columnIdx = Integer.parseInt(_columnStr);
                selectedColumns.add(hiveColumnsArray[_columnIdx]);
            }
            
            return selectedColumns;
        } else {
            return null;
        }
    }
    
    public static void copyJDBCProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties tableProp = tableDesc.getProperties();
        
        for (String requiredField : REQUIRED_CONF) {
            if (Util.isEmpty(tableProp.getProperty(requiredField))) {
                throw new IllegalArgumentException("Property " + requiredField + " is required.");
            }
        }
        
        for (Entry<Object, Object> entry : tableProp.entrySet()) {
            jobProperties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        
        /*for (String configKey : DEFAULT_REQUIRED_PROPERTIES) {
            String propertyKey = configKey.getPropertyName();
            if ((props == null) || (!props.containsKey(propertyKey)) || (isEmptyString(props.getProperty(propertyKey)))) {
                throw new IllegalArgumentException("Property " + propertyKey + " is required.");
            }
        }*/
    }

}
