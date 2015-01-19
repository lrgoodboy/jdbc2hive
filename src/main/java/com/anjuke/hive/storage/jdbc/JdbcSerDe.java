package com.anjuke.hive.storage.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSerDe  implements SerDe {
    
    private StructObjectInspector objectInspector;
    private List<String> dbColumnNames;
    private List<String> row;
    private int numColumns;
    
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSerDe.class);

    @Override
    public Object deserialize(Writable obj) throws SerDeException {
        if (!(obj instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable : " + obj.getClass().getName());
        }
        
        row.clear();
        MapWritable mapObj = (MapWritable) obj;
        Text columnKey = new Text();

        for (int i = 0; i < numColumns; i++) {
            columnKey.set(dbColumnNames.get(i).toLowerCase());
            Writable value = mapObj.get(columnKey);
            if (value == null) {
                row.add(null);
            } else {
                row.add(value.toString());
            }
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return new SerDeStats();
    }

    @Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException {
        LOG.info(" _____ jdbc inited " + tbl.toString());
        
        if (!tbl.containsKey(HiveConfiguration.JDBC_DRIVER_CLASS)) {
            return ;
        }
        
        String hiveColumnStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String hiveColumnTypeStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        
        String[] hiveColumns = hiveColumnStr.split(",");
        String[] hiveColumnTypes = hiveColumnTypeStr.split(":");
        
        System.out.println("jdbc2hive " +   hiveColumnStr + " " + hiveColumnTypeStr);
        
        numColumns = hiveColumns.length;
        if (numColumns == 0) {
            LOG.error("can not get hive columns");
            throw new SerDeException("can not get hive columns");
        }
        
        if (numColumns != hiveColumnTypes.length) {
            LOG.error("num of columns not equal num of column types " + hiveColumnStr + " " +  hiveColumnTypeStr);
            throw new SerDeException("num of columns not equal num of column types");
        }
        
        List<String> hiveColumnList = Arrays.asList(hiveColumns);
        
        // copy properties to conf.
        for (Entry<Object, Object> entry : tbl.entrySet()) {
            conf.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        
        dbColumnNames = HiveConfiguration.getInstance(conf).getDBSelectFields(hiveColumnList);
        
        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            // all use string is not a good solution.
            fieldInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        
        row = new ArrayList<String>(numColumns);
        objectInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(hiveColumnList,
                                                                        fieldInspectors);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public Writable serialize(Object arg0, ObjectInspector arg1)
            throws SerDeException {
        throw new SerDeException("Serialize to db does not supported. Pull-request is appreciated.");
    }

}
