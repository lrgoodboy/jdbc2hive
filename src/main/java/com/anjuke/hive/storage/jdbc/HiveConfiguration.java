package com.anjuke.hive.storage.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;

public class HiveConfiguration {
    
    public final static String CONF_TABLENAME = "";
    
    public final static String CONF_SPLITEDBY = "splited.by";

    public static final String JDBC_URL = "";

    public static final String JDBC_DRIVER_CLASS = "";

    public static final String DBCP_CONFIG_PREFIX = ""; 
    
    public static List<String> getDBSelectFields(Configuration conf, List<String> hiveColumns) {
        conf.get("");
        return hiveColumns;
    }
    
    public static String getSplitedBy(Configuration conf) {
        return conf.get(CONF_SPLITEDBY);
    }
    
    public static long getBlockSize(Configuration conf) {
        return conf.getLong("dfs.blocksize", 67108864);
    }
    
    public static ExprNodeDesc getExpNodeDesc (Configuration conf) {
        String filterXml = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        return Utilities.deserializeExpression(filterXml, conf);
    }
    
    public static List<String> getHiveSelectedColumns(Configuration conf) {
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

}
