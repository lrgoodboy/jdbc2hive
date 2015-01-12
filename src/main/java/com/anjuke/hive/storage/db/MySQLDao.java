package com.anjuke.hive.storage.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.mockito.internal.util.reflection.Fields;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;
import com.anjuke.hive.storage.jdbc.Util;

public class MySQLDao implements Dao {
    
    private List<String> selectFields;
    
    private String selectFieldsStr;
    
    private String condition;
    
    private ExprNodeDesc conditionNode;
    
    private Connection connection;
    
    private String tableName;
    
    private JdbcInputSplit split;

    @Override
    public void setExpnode(ExprNodeDesc conditionNode) {
        this.conditionNode = conditionNode;
    }

    @Override
    public void setSplit(JdbcInputSplit split) {
        this.split = split;
    }
    
    public String getSplitCondition() {
        if (split == null) {
            return null;
        }
        
        String splitCondition = "";
        String lowerCause = split.getLowerCause();
        if (lowerCause != null && !lowerCause.trim().isEmpty()) {
            splitCondition = "(" + lowerCause + ")"; 
        }
        
        String upperCause = split.getUpperCause();
        if (upperCause != null && !upperCause.trim().isEmpty()) {
            if (!splitCondition.trim().isEmpty()) {
                splitCondition += " AND (" + upperCause + ")";
            }
        }
        
        if (!splitCondition.trim().isEmpty()) {
            return "(" + splitCondition + ")";
        }
        
        return null;
    }

    @Override
    public void setSelectFields(List<String> fields) {
        selectFields = fields;
        
        String selectFieldsStr = "";
        for (String field : fields) {
            if (!selectFieldsStr.isEmpty()) {
                selectFieldsStr += ",";
            }
            
            selectFieldsStr += "`" + field + "`";
        }
        
        this.selectFieldsStr = selectFieldsStr;
    }

    @Override
    public String getBaseCondition() {
        if (Util.isEmpty(condition)) {
            return "";
        } else {
            return condition;
        }
    }
    
    private String buildWhere(String ... condition) {
        String where = "";
        for (int i=0; i<condition.length; i++) {
            if (Util.isEmpty(condition[i])) {
                continue;
            }
            
            if (!Util.isEmpty(where)) {
                where += " AND ";
            }
            
            where += condition[i];
        }
        
        if (!Util.isEmpty(where)) {
            where = " WHERE " + where;
        }
        
        return where;
    }

    @Override
    public long getAffetcedRows() {
        // execute explan to get affetecd rows.
        long rows = 0;
        
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "EXPLAIN SELECT " + this.selectFieldsStr 
                    + " FROM " + this.tableName + " " + buildWhere(getBaseCondition()));
            ResultSet rs = ps.executeQuery();
            
            rows = rs.getLong(9);
            
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return rows;
    }

    @Override
    public int getRowDataLength() {
        // summary every field's length.
        // use desc table to get every field's length
        int length = 0;
        
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "SELECT " + this.selectFieldsStr 
                    + " FROM " + this.tableName + " LIMIT 1");
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            for (int i=1; i<=columnCount; i++) {
                length += metaData.getColumnDisplaySize(i);
            }
            
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            length = selectFields.size() * 10;
        }
        
        return length;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = "`" + tableName + "`";
    }

    @Override
    public void setConnnection(Connection conn) {
        this.connection = conn;
    }

    @Override
    public Bound getConditionBound(Bound bound) {
        
        try {
            String boundField = "`" + bound.getField() + "`";
            PreparedStatement ps = connection.prepareStatement(
                    "(SELECT " + boundField 
                    + " FROM " + this.tableName + " " + buildWhere(getBaseCondition())
                    + " ORDER BY " + boundField + " DESC LIMIT 1)"
                    + " UNOIN ALL"
                    + "(SELECT " + boundField 
                    + " FROM " + this.tableName + " " + buildWhere(getBaseCondition())
                    + " ORDER BY " + boundField + " ASC LIMIT 1)"
                    );
            ResultSet rs = ps.executeQuery();
            
            if (rs.next()) {
                bound.setType(rs.getMetaData().getColumnType(1));
                bound.setUpper(rs.getObject(1));
            }
            
            if (rs.next()) {
                bound.setLower(rs.getObject(1));
            }
            
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return bound;
    }
    
    public JdbcRecordIterator getRecordIterator() {
        try {
            
            String sql = "SELECT " + this.selectFieldsStr 
                    + " FROM " + this.tableName 
                    + " " + buildWhere(getBaseCondition(), getSplitCondition());
            
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            
            return new JdbcRecordIterator(connection, ps, rs);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        
    }

}
