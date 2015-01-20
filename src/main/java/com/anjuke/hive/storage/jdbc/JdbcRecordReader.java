package com.anjuke.hive.storage.jdbc;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.db.Dao;
import com.anjuke.hive.storage.db.DaoFactory;
import com.anjuke.hive.storage.db.JdbcRecordIterator;
import com.anjuke.hive.storage.parser.NodeProcessor;

public class JdbcRecordReader implements RecordReader<LongWritable, MapWritable>  {
    
    public JdbcRecordReader(InputSplit split, Configuration conf, Reporter reporter) {
        this.split = (JdbcInputSplit) split;
        this.conf = conf;
        this.reporter = reporter;
    }

    private JdbcInputSplit split;
    private Configuration conf;
    private Dao dao;
    private Reporter reporter; 
    private JdbcRecordIterator iterator;
    private long pos = 0;
    private boolean isTrimNewLine = true;
    
    @Override
    public boolean next(LongWritable key, MapWritable value) throws IOException {
        // use split to generate RecorderReader to read data
        if (iterator == null) {
            dao = DaoFactory.getDao(conf);
            HiveConfiguration hiveConf = HiveConfiguration.getInstance(conf);

            dao.setTableName(hiveConf.getTableName());
            dao.setConditionNode(hiveConf.getExpNodeDesc(), hiveConf.getColumnMap());
            dao.setSelectFields(hiveConf.getDBSelectFields(hiveConf.getHiveSelectedColumns()));
            dao.setSplit(split);
            
            iterator = dao.getRecordIterator();
            isTrimNewLine = hiveConf.isTrimNewLine();
        }
        
        if (iterator.hasNext()) {
            Map<String, String> dbValues = iterator.next();
            
            if ((dbValues != null) && (!dbValues.isEmpty())) {
                key.set(++pos);
                
                for (Entry<String, String> entry : dbValues.entrySet()) {
                    // hive use lower case column names
                    Text _key = new Text(entry.getKey().toLowerCase());
                    Writable _value = processValue(entry.getValue());
                    value.put(_key, _value);
                }
                
                return true;
            }
            
            return false;
        }
        
        return false;
    }
    
    private Writable processValue(String value) {
        if (value == null) {
            return NullWritable.get();
        }
        
        if (isTrimNewLine) {
            value = value.replaceAll("\r|\n|\1", "");
        } else {
            value = value.replaceAll("\1", "");
        }
        
        return new Text(value);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public MapWritable createValue() {
        return new MapWritable();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public float getProgress() throws IOException {
        float progress;
        if (split.getLength() <=0 ) {
            progress = 0.0f;
        } else {
            progress = pos / split.getLength();
            progress = progress > 1.0f ? 1.0f : progress;
        }
        
        return progress;
    }

}
