package com.anjuke.hive.storage.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;
import com.anjuke.hive.storage.jdbc.Util;
import com.anjuke.hive.storage.parser.NodeProcessor;

public class MySQLDao implements Dao {
    
    private List<String> selectFields;
    private String selectFieldsStr;
    private String condition;
    private ExprNodeDesc conditionNode;
    private Connection connection;
    private String tableName;
    private JdbcInputSplit split;
    
    private static final Logger LOG = LoggerFactory.getLogger(MySQLDao.class);

    @Override
    public void setConditionNode(ExprNodeDesc conditionNode, Map<String, String> columnMap) {
        this.conditionNode = conditionNode;
        
        if (conditionNode != null) {
            // in fact, every database has related NodeProcessor.
            ExprNodeDesc parsedNode = NodeProcessor
                    .getNodeProcessor(conditionNode)
                    .parseNode(conditionNode, columnMap, 0);
            
            if (parsedNode != null) {
                this.condition = parsedNode.getExprString();
            }
        } else {
            this.condition = "";
        }
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
            String sql = "EXPLAIN SELECT " + this.selectFieldsStr 
                    + " FROM " + this.tableName + " " + buildWhere(getBaseCondition());
            LOG.info("affected rows sql : " + sql);
            
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                rows = rs.getLong(9);
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        LOG.info("affected rows : " + rows);
        return rows;
    }

    @Override
    public int getRowDataLength() {
        // summary every field's length.
        // use desc table to get every field's length
        int length = 0;
        
        try {
            String sql = "SELECT " + this.selectFieldsStr 
                    + " FROM " + this.tableName + " LIMIT 1";
            LOG.info("detect rowdata length sql : " + sql);
            
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            for (int i=1; i<=columnCount; i++) {
                switch (metaData.getColumnType(i)) {
                case Types.BIGINT:
                    length += 8;
                    break;
                    
                case Types.INTEGER:
                    length += 4;
                    break;
                    
                case Types.SMALLINT:
                    length += 2;
                    break;
                    
                case Types.TINYINT:
                    length += 1;
                    break;
                    
                case Types.FLOAT:
                case Types.DOUBLE:
                    length += 4;
                    break;
                    
                case Types.DECIMAL:
                    length += 8;
                    break;
                    
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    length += metaData.getColumnDisplaySize(i) * 3;
                    break;
                    
                case Types.TIME:
                case Types.DATE:
                    length += 3;
                    break;

                case Types.TIMESTAMP:
                    length += 4;
                    break;
                    
                default: 
                    length += metaData.getColumnDisplaySize(i);
                    break;
                }
                
            }
            
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            length = selectFields.size() * 10;
        }
        
        LOG.info("row length: " + length);
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
            String sql = "SELECT " + boundField 
                    + " FROM " + this.tableName + " " + buildWhere(getBaseCondition())
                    + " ORDER BY " + boundField + " DESC LIMIT 1";
            LOG.info(sql);
            
            PreparedStatement ps ;
            ResultSet rs ;
            
            // get upper bound.
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            
            if (rs.next()) {
                bound.setType(rs.getMetaData().getColumnType(1));
                bound.setUpper(rs.getObject(1));
            }
            
            rs.close();
            ps.close();
            
            // get lower bound.
            sql =  "SELECT " + boundField 
                    + " FROM " + this.tableName + " " + buildWhere(getBaseCondition())
                    + " ORDER BY " + boundField + " ASC LIMIT 1";
            LOG.info(sql);
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            
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
    
    @Override
    public JdbcRecordIterator getRecordIterator() {
        try {
            
            String sql = "SELECT " + this.selectFieldsStr 
                    + " FROM " + this.tableName 
                    + " " + buildWhere(getBaseCondition(), getSplitCondition());
            LOG.info("getRecordIterator sql : " + sql);
            
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            
            return new JdbcRecordIterator(connection, ps, rs);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        
    }

}
