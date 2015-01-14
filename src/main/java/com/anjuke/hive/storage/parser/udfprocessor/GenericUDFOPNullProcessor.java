package com.anjuke.hive.storage.parser.udfprocessor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;

import com.anjuke.hive.storage.parser.ExprNodeGenericFuncDescProcessor;
import com.anjuke.hive.storage.parser.NodeProcessor;

public class GenericUDFOPNullProcessor extends ExprNodeGenericFuncDescProcessor {

    @Override
    public ExprNodeDesc parseUDFNode(ExprNodeGenericFuncDesc expNode, Map<String, String> columnMap, int level) {
        ExprNodeDesc child = expNode.getChildren().get(0);
        ExprNodeDesc childNode = NodeProcessor.getNodeProcessor(child).parseNode(child, columnMap, level+1);
        
        // exp is null !exp  exp is null
        // if we cannot explain exp, we choose return null
        if (childNode == null) {
            return null;
        }
        
        // use new childNode
        expNode.setChildExprs(Arrays.asList(new ExprNodeDesc[] {childNode}));
        
        return expNode;
    }

}
