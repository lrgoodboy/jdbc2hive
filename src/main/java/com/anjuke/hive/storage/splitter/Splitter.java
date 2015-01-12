package com.anjuke.hive.storage.splitter;

import java.util.List;

import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.jdbc.Bound;

public interface Splitter {
    
    public List<InputSplit> getSplits(long totalRows, int rowLenth, Bound bound, long blockSize);

}
