package com.anjuke.hive.storage.splitter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;

public class LongSplitter implements Splitter {

    @Override
    public List<JdbcInputSplit> getSplits(long totalRows, int rowLenth, Bound bound, long blockSize, Path[] tablePaths) {
        
        long lower = Long.parseLong(bound.getLower().toString());
        long upper = Long.parseLong(bound.getUpper().toString());
        String field = bound.getField();
        
        if (totalRows == 0) {
            totalRows = upper - lower;
        }
        
        int numSplits = (int) Math.round(totalRows * rowLenth * 1.0 / blockSize);
        // one map per million records. 
        numSplits = (int) Math.max(numSplits, Math.round(totalRows / 1000000.0));
        
        if (numSplits < 1) {
            numSplits = 1;
        }
        
        long step = (long) ((upper - lower) / numSplits);
        int remain = (int) ((upper - lower) % numSplits);
        
        long start = lower;
        long end;
        
        List<JdbcInputSplit> splits = new ArrayList<JdbcInputSplit>(numSplits);
        
        for (int i=1; i<=numSplits; i++) {
            end = start + step;
            if (remain > 0) {
                end ++;
                remain --;
            }
            
            // generate split;
            JdbcInputSplit split = new JdbcInputSplit(tablePaths[0]);
            split.setLength(end - start);
            
            String lowerCause = null;
            String upperCause = null;
            
            lowerCause = field + " >= " + start;
            
            if (i == numSplits) {
                assert (end == upper);
                upperCause = field + " <= " + end;
            } else {
                upperCause = field + " < " + end;
            }
            
            split.setLowerCause(lowerCause);
            split.setUpperCause(upperCause);
            
            splits.add(split);
            
            start = end;
        }
        
        return splits;
    }

}
