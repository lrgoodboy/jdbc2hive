package com.anjuke.hive.storage.splitter;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;

public class DateTimeSplitter implements Splitter {

    @Override
    public List<JdbcInputSplit> getSplits(long totalRows, int rowLenth, Bound bound, long blockSize, Path[] tablePaths) {
        // TODO Auto-generated method stub
        return null;
    }

}
