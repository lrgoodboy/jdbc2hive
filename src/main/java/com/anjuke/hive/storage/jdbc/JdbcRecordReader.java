package com.anjuke.hive.storage.jdbc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.db.Dao;
import com.anjuke.hive.storage.db.DaoFactory;

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
    
    @Override
    public boolean next(LongWritable key, MapWritable value) throws IOException {
        // use split to generate RecorderReader to
        
        if (dao == null) {
            dao = DaoFactory.getDao(conf);
            
            dao.setExpnode(HiveConfiguration.getExpNodeDesc(conf));
            dao.setSelectFields(HiveConfiguration.getDBSelectFields(conf, HiveConfiguration.getHiveSelectedColumns(conf)));
            dao.setSplit(split);
            
            //dao.setFetchBound(split.getBound())
        }
        
        return false;
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
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

}
