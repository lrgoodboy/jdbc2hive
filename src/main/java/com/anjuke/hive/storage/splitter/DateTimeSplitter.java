package com.anjuke.hive.storage.splitter;

import java.util.List;

import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.jdbc.Bound;

public class DateTimeSplitter implements Splitter {

    @Override
    public List<InputSplit> getSplits(long totalRows, int rowLenth, Bound bound, long blockSize) {
        // TODO Auto-generated method stub
        return null;
    }

}
