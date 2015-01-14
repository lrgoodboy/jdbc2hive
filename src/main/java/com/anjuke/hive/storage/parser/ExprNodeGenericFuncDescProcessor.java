package com.anjuke.hive.storage.parser;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;

import com.anjuke.hive.storage.parser.udfprocessor.GenericUDFOPAndProcessor;
import com.anjuke.hive.storage.parser.udfprocessor.GenericUDFOPNullProcessor;
import com.anjuke.hive.storage.parser.udfprocessor.GenericUDFBinaryOPProcessor;

public class ExprNodeGenericFuncDescProcessor extends NodeProcessor {
    
    private static Map<Class<? extends GenericUDF>, ExprNodeGenericFuncDescProcessor> processorMap
        = new HashMap<Class<? extends GenericUDF>, ExprNodeGenericFuncDescProcessor>();
    static {
        processorMap.put(GenericUDFOPAnd.class, new GenericUDFOPAndProcessor());
        processorMap.put(GenericUDFOPOr.class, new GenericUDFBinaryOPProcessor());
        
        processorMap.put(GenericUDFOPEqualOrGreaterThan.class, new GenericUDFBinaryOPProcessor());
        processorMap.put(GenericUDFOPEqualOrLessThan.class, new GenericUDFBinaryOPProcessor());
        processorMap.put(GenericUDFOPGreaterThan.class, new GenericUDFBinaryOPProcessor());
        processorMap.put(GenericUDFOPLessThan.class, new GenericUDFBinaryOPProcessor());
        
        
        processorMap.put(GenericUDFOPEqual.class, new GenericUDFBinaryOPProcessor());
        
        // it does not support push not equal condition to jdbc
        processorMap.put(GenericUDFOPNotEqual.class, new ExprNodeGenericFuncDescProcessor());
        
        processorMap.put(GenericUDFOPNot.class, new GenericUDFOPNullProcessor());
        processorMap.put(GenericUDFOPNotNull.class, new GenericUDFOPNullProcessor());
        processorMap.put(GenericUDFOPNull.class, new GenericUDFOPNullProcessor());
    }

    @Override
    public ExprNodeDesc parseNode(ExprNodeDesc expNode, Map<String, String> columnMap, int level) {
        ExprNodeGenericFuncDesc node = (ExprNodeGenericFuncDesc) expNode;
        ExprNodeGenericFuncDescProcessor processor = processorMap.get(node.getGenericUDF().getClass());
        
        // does not support this UDF.
        if (processor == null) {
            return null;
        }
        
        return processor.parseUDFNode(node, columnMap, level + 1);
    }
    
    public ExprNodeDesc parseUDFNode(ExprNodeGenericFuncDesc expNode, Map<String, String> columnMap, int level) {
        return null;
    }

}
