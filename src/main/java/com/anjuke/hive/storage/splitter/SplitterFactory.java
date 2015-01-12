package com.anjuke.hive.storage.splitter;

import java.sql.Types;

import com.anjuke.hive.storage.jdbc.Bound;

public class SplitterFactory {
    
    public static Splitter getSplitter(Bound bound) {
        switch (bound.getType()) {
        
        case Types.INTEGER:
        case Types.BIGINT:
            return new LongSplitter();
            
        case Types.TIMESTAMP:
            return new DateTimeSplitter();
        }
        
        return null;
    }

}
