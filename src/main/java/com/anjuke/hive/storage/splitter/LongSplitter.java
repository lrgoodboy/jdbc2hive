package com.anjuke.hive.storage.splitter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.InputSplit;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;

public class LongSplitter implements Splitter {

    @Override
    public List<InputSplit> getSplits(long totalRows, int rowLenth, Bound bound, long blockSize) {
        
        long lower = (Long)(bound.getLower());
        long upper = (Long)(bound.getUpper());
        
        if (totalRows == 0) {
            totalRows = upper - lower;
        }
        
        int numSplits = (int) (totalRows * rowLenth / blockSize);
        
        List<InputSplit> splits = new ArrayList<InputSplit>();
        if (numSplits <= 1) {
            JdbcInputSplit split = new JdbcInputSplit();
            split.setLowerCause(null);
            split.setUpperCause(null);
            split.setLength(totalRows);
            
            splits.add(split);
            
            return splits;
        }
        
        long step = (long) ((upper - lower) / numSplits);
        int remain = (int) ((upper - lower) % numSplits);
        
        long start = lower;
        long end; 
        
        String field = bound.getField();
        
        for (int i=1; i<=numSplits; i++) {
            end = start + step;
            if (remain > 0) {
                end ++;
                remain --;
            }
            
            assert (end == upper);
            
            // generate split;
            JdbcInputSplit split = new JdbcInputSplit();
            split.setLength(end - start);
            
            String lowerCause = null;
            String upperCause = null;
            
            lowerCause = field + " >= " + start;
            
            if (i == numSplits) {
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
