package com.anjuke.hive.storage.jdbc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.anjuke.hive.storage.db.Dao;
import com.anjuke.hive.storage.db.DaoFactory;
import com.anjuke.hive.storage.parser.NodeProcessor;
import com.anjuke.hive.storage.splitter.Splitter;
import com.anjuke.hive.storage.splitter.SplitterFactory;

public class JdbcInputFormat  extends HiveInputFormat<LongWritable, MapWritable> {

    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf conf,
            Reporter reporter) throws IOException {
        return new JdbcRecordReader(split, conf, reporter);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int num) throws IOException {
        HiveConfiguration hiveConf = HiveConfiguration.getInstance(conf);
        Dao dao = DaoFactory.getDao(conf);
        
        dao.setConditionNode(hiveConf.getExpNodeDesc(), hiveConf.getColumnMap());
        dao.setSelectFields(hiveConf.getDBSelectFields(hiveConf.getHiveSelectedColumns()));
        dao.setTableName(hiveConf.getTableName());
        
        Bound bound = new Bound(hiveConf.getSplitedBy());
        bound = dao.getConditionBound(bound);
        
        long totalRows = dao.getAffetcedRows();
        int rowLenth = dao.getRowDataLength();
        
        Path[] tablePaths = FileInputFormat.getInputPaths(conf);
        
        Splitter splitter = SplitterFactory.getSplitter(bound);
        if (splitter == null) {
            return getOneInputSplit(totalRows, tablePaths[0]);
        }
        
        List<JdbcInputSplit> splits = splitter.getSplits(totalRows, rowLenth, bound, hiveConf.getBlockSize(), tablePaths);
        if (splits != null) {
            return (InputSplit[]) (splits.toArray(new InputSplit[splits.size()]));
        } else {
            return getOneInputSplit(totalRows, tablePaths[0]);
        }
    }
    
    
    private InputSplit[] getOneInputSplit(long totalRows, Path path) {
        JdbcInputSplit split = new JdbcInputSplit(path);
        split.setLowerCause("");
        split.setUpperCause("");
        split.setLength(totalRows);
        
        return new InputSplit[]{split};
    }

}
