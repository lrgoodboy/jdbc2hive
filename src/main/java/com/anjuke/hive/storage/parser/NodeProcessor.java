package com.anjuke.hive.storage.parser;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;

public abstract class NodeProcessor {
    
    private static Map<Class<? extends ExprNodeDesc>, NodeProcessor> processorMap 
        = new HashMap<Class<? extends ExprNodeDesc>, NodeProcessor>();
    static {
        processorMap.put(ExprNodeColumnDesc.class, new ExprNodeColumnDescProcessor());
        processorMap.put(ExprNodeConstantDesc.class, new DefaultProcessor());
        processorMap.put(ExprNodeFieldDesc.class, new NullProcessor());
        processorMap.put(ExprNodeGenericFuncDesc.class, new ExprNodeGenericFuncDescProcessor());
        processorMap.put(ExprNodeNullDesc.class, new NullProcessor());
    }

    public abstract ExprNodeDesc parseNode(ExprNodeDesc expNode, Map<String, String> columnMap, int level);

    public static NodeProcessor getNodeProcessor(ExprNodeDesc expNode) {
        NodeProcessor processor = processorMap.get(expNode.getClass());
        if (processor == null) {
            processor = processorMap.get(ExprNodeNullDesc.class.getName());
        }
        
        return processor;
    }

}
