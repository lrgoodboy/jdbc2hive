package com.anjuke.hive.storage.parser;

import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;

public class DefaultProcessor extends NodeProcessor {

    @Override
    public ExprNodeDesc parseNode(ExprNodeDesc expNode, Map<String, String> columnMap, int level) {
        return expNode;
    }

}
