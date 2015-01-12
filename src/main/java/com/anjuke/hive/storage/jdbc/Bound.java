package com.anjuke.hive.storage.jdbc;

import java.io.Serializable;

public class Bound implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -4952921597034209861L;

    public Bound(String field) {
        this.field = field;
    }
    
    public Bound(Object upper, Object lower, int type, boolean isSigned) {
        super();
        this.upper = upper;
        this.lower = lower;
        this.type = type;
        this.isSigned = isSigned;
    }
    
    private String field;

    private Object upper;
    
    private Object lower;
    
    private int type;
    
    private boolean isSigned;

    public Object getUpper() {
        return upper;
    }

    public void setUpper(Object value) {
        this.upper = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public boolean isSigned() {
        return isSigned;
    }

    public void setSigned(boolean isSigned) {
        this.isSigned = isSigned;
    }

    public Object getLower() {
        return lower;
    }

    public void setLower(Object lower) {
        this.lower = lower;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
}
