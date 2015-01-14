package com.anjuke.hive.storage.db;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;

public interface Dao {
    
    public void setTableName(String tableName);
    
    public void setConnnection(Connection conn);
    
    /* 设置表达式节点, convert node to condition */
    public void setConditionNode(ExprNodeDesc conditionNode, Map<String, String> columnMap);
      
    /* 在 mr 具体的读数据的 task 里面需要设置  */
    public void setSplit(JdbcInputSplit bound) ;

    /* 设置需要的字段 */
    public void setSelectFields(List<String> fields);
    
    /* 获得条件对应的 Bound，即按什么字段，在哪个区间？ */
    public Bound getConditionBound(Bound bound);

    /* 来自 expnode 对应的condition */
    public String getBaseCondition();

    /* 获得影响的条数， 一般在生成 split 时需要 */
    public long getAffetcedRows();

    /* 获得一行数据的大小  */
    public int getRowDataLength();
    
    /* 获得读取数据的 Iterator */
    public JdbcRecordIterator getRecordIterator();
}
