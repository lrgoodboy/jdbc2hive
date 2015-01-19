package com.anjuke.hive.storage.jdbc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class JdbcInputSplit extends FileSplit implements InputSplit {
    
    // it is better to use Bound, but de/serialized Bound is complex.  
    private String lowerCause;
    private String upperCause;
    
    private long length;
    private static final String[] EMPTY_ARRAY = new String[] {};
    
    public JdbcInputSplit() {
        super((Path) null, 0, 0, EMPTY_ARRAY);
    }
    
    public JdbcInputSplit(Path path) {
        super((Path) path, 0, 0, EMPTY_ARRAY);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        lowerCause = Text.readString(in);
        upperCause = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, lowerCause);
        Text.writeString(out, upperCause);
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException {
        return EMPTY_ARRAY;
    }
    
    public void setLength(long length) {
        this.length = length;
    }
    
    public String getLowerCause() {
        return lowerCause;
    }

    public void setLowerCause(String lowerCause) {
        this.lowerCause = lowerCause;
    }

    public String getUpperCause() {
        return upperCause;
    }

    public void setUpperCause(String upperCause) {
        this.upperCause = upperCause;
    }

}
