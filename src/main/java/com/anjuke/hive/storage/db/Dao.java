package com.anjuke.hive.storage.db;

import java.sql.Connection;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;

public interface Dao {
    
    public void setTableName(String tableName);
    
    public void setConnnection(Connection conn);
    
    /* 设置表达式节点, convert node to condition */
    public void setExpnode(ExprNodeDesc conditionNode);
      
    /* 在 mr 具体的读数据的 task 里面需要设置  */
    public void setSplit(JdbcInputSplit bound) ;

    /**
     * set fields needed in  map reduce task 
     * 
     * @param fields
     */
    public void setSelectFields(List<String> fields);
    
    public Bound getConditionBound(Bound bound);

    /* 来自 expnode 对应的condition */
    public String getBaseCondition();

    /* 获得影响的条数， 一般在生成 split 时需要 */
    public long getAffetcedRows();

    /* 获得一行数据的大小  */
    public int getRowDataLength();
    
    public JdbcRecordIterator getRecordIterator();
}
