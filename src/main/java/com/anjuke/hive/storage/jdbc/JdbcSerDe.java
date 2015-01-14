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

public class JdbcSerDe  implements SerDe {
    
    private StructObjectInspector objectInspector;
    private List<String> dbColumnNames;
    private List<String> row;
    private int numColumns;

    @Override
    public Object deserialize(Writable obj) throws SerDeException {
        if (!(obj instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable : " + obj.getClass().getName());
        }
        
        MapWritable mapObj = (MapWritable) obj;
        Text columnKey = new Text();

        for (int i = 0; i < numColumns; i++) {
            columnKey.set(dbColumnNames.get(i));
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
        
        if (!tbl.contains(HiveConfiguration.JDBC_DRIVER_CLASS)) {
            return ;
        }
        
        String hiveColumnStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String hiveColumnTypeStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        
        String[] hiveColumns = hiveColumnStr.split(",");
        String[] hiveColumnTypes = hiveColumnTypeStr.split(":");
        
        numColumns = hiveColumns.length;
        if (numColumns == 0) {
            throw new SerDeException("can not get hive columns");
        }
        
        if (numColumns != hiveColumnTypes.length) {
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
