package com.anjuke.hive.storage.jdbc;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.anjuke.hive.storage.db.Dao;
import com.anjuke.hive.storage.db.DaoFactory;
import com.anjuke.hive.storage.splitter.Splitter;
import com.anjuke.hive.storage.splitter.SplitterFactory;

public class JdbcInputFormat  extends HiveInputFormat<LongWritable, MapWritable> {

    @SuppressWarnings("unchecked")
    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf conf,
            Reporter reporter) throws IOException {
        return new JdbcRecordReader(split, conf, reporter);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int num) throws IOException {
        HiveConfiguration hiveConf = HiveConfiguration.getInstance(conf);
        Dao dao = DaoFactory.getDao(conf);
        
        Bound bound = new Bound(hiveConf.getSplitedBy());
        
        dao.setExpnode(hiveConf.getExpNodeDesc());
        dao.setSelectFields(hiveConf.getDBSelectFields(hiveConf.getHiveSelectedColumns()));
        
        bound = dao.getConditionBound(bound);
        
        long totalRows = dao.getAffetcedRows();
        int rowLenth = dao.getRowDataLength();
        
        Splitter splitter = SplitterFactory.getSplitter(bound);
        
        return (InputSplit[]) splitter.getSplits(totalRows, rowLenth, bound, hiveConf.getBlockSize()).toArray();
    }

}
