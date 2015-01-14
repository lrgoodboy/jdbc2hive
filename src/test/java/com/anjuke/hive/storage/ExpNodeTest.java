package com.anjuke.hive.storage;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import com.anjuke.hive.storage.parser.NodeProcessor;

public class ExpNodeTest {
    
    private String exp1xml = "";
    
    @Before
    public void testRead() throws IOException {
        InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("expnode1.xml");
        BufferedReader bf = new BufferedReader(new InputStreamReader(ins, "utf-8"));
        
        exp1xml = "";
        String tmp;
        while ((tmp = bf.readLine()) != null) {
            exp1xml += tmp;
        }
        bf.close();
        ins.close();
    }
    
    @Test
    public void testDeserialized() {
        Configuration conf = new Configuration();
        
        ExprNodeDesc expNode = Utilities.deserializeExpression(exp1xml, conf);
        walkNode(expNode, 0);
        assertNotNull(expNode);
        
        System.out.println("after parse");
        
        ExprNodeDesc newExpNode = NodeProcessor.getNodeProcessor(expNode).parseNode(expNode, null, 0);
        walkNode(newExpNode, 0);
        assertNotNull(newExpNode);
    }
    
    public void walkNode(ExprNodeDesc expNode, int level) {
        if (expNode == null) {
            return;
        }
        
        String prefix = "";
        for (int i=0; i<level; i++) {
            prefix += "\t";
        }
        
        
        System.out.print(prefix + expNode.getName() + " " + expNode.getExprString() + " type: " + expNode.getTypeString());
        if (expNode instanceof ExprNodeGenericFuncDesc) {
            ExprNodeGenericFuncDesc x = (ExprNodeGenericFuncDesc) expNode;
            System.out.print(" " + x.getGenericUDF());
        }
        
        System.out.println();
        
        if (expNode.getChildren() != null) {
            for (ExprNodeDesc node : expNode.getChildren()) {
                walkNode(node, level+1);
            }
        }
    }

}
