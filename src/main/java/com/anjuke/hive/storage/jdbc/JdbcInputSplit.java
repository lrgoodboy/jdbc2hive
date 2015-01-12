package com.anjuke.hive.storage.jdbc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

public class JdbcInputSplit implements InputSplit {
    
    // it is better to use Bound, but de/serialized Bound is complex.  

    private String lowerCause;
    
    private String upperCause;
    
    private long length;

    @Override
    public void readFields(DataInput in) throws IOException {
        lowerCause = Text.readString(in);
        upperCause = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, lowerCause);
        Text.writeString(out, upperCause);
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException {
        return null;
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
