package com.anjuke.hive.storage.parser;

import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;

public class NullProcessor extends ExprNodeGenericFuncDescProcessor {

    @Override
    public ExprNodeDesc parseUDFNode(ExprNodeGenericFuncDesc expNode,
            Map<String, String> columnMap, int level) {
        return null;
    }

    @Override
    public ExprNodeDesc parseNode(ExprNodeDesc expNode, Map<String, String> columnMap, int level) {
        return null;
    }

}
