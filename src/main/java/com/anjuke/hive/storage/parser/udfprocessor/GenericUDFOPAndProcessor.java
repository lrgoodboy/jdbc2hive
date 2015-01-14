package com.anjuke.hive.storage.parser.udfprocessor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;

import com.anjuke.hive.storage.parser.ExprNodeGenericFuncDescProcessor;
import com.anjuke.hive.storage.parser.NodeProcessor;

public class GenericUDFOPAndProcessor extends ExprNodeGenericFuncDescProcessor {

    @Override
    public ExprNodeDesc parseUDFNode(ExprNodeGenericFuncDesc expNode, Map<String, String> columnMap, int level) {
        List<ExprNodeDesc> childNodes = expNode.getChildren();
        ExprNodeDesc left = childNodes.get(0);
        ExprNodeDesc right = childNodes.get(1);
        
        ExprNodeDesc leftNode = NodeProcessor.getNodeProcessor(left).parseNode(left, columnMap, level+1);
        ExprNodeDesc rightNode = NodeProcessor.getNodeProcessor(right).parseNode(right, columnMap, level+1);
        
        if (leftNode == null) {
            return rightNode;
        } else {
            if (rightNode == null) {
                return leftNode;
            }
        }
        
        // use new leftNode and rightNode
        expNode.setChildExprs(Arrays.asList(new ExprNodeDesc[] {leftNode, rightNode}));
        
        return expNode;
    }

}
