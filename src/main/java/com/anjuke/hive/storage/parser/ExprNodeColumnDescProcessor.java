package com.anjuke.hive.storage.parser;

import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;

public class ExprNodeColumnDescProcessor extends NodeProcessor {

    @Override
    public ExprNodeDesc parseNode(ExprNodeDesc expNode, Map<String, String> columnMap, int level) {
        ExprNodeColumnDesc node = (ExprNodeColumnDesc) expNode;
        if (columnMap != null && columnMap.get(node.getColumn()) != null) {
            node.setColumn(columnMap.get(node.getColumn()));
        }
        
        return node;
    }

}
