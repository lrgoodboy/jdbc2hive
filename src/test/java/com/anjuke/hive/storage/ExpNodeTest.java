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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import com.anjuke.hive.storage.parser.NodeProcessor;

public class ExpNodeTest {
    
    private String exp1xml = "";
    private String exp2xml = "";
    private String exp3xml = "";
    
    @Before
    public void testRead() throws IOException {
        String tmp;
        InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("expnode1.xml");
        BufferedReader bf = new BufferedReader(new InputStreamReader(ins, "utf-8"));
        
        exp1xml = "";
        while ((tmp = bf.readLine()) != null) {
            exp1xml += tmp;
        }
        
        bf.close();
        ins.close();
        
        ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("expnode2.xml");
        bf = new BufferedReader(new InputStreamReader(ins, "utf-8"));
        
        exp2xml = "";
        while ((tmp = bf.readLine()) != null) {
            exp2xml += tmp;
        }
        
        bf.close();
        ins.close();
        
        ins = Thread.currentThread().getContextClassLoader().getResourceAsStream("expnode3.xml");
        bf = new BufferedReader(new InputStreamReader(ins, "utf-8"));
        
        exp3xml = "";
        while ((tmp = bf.readLine()) != null) {
            exp3xml += tmp;
        }
        
        bf.close();
        ins.close();
    }
    
    @Test
    public void testDeserialized1() {
        Configuration conf = new Configuration();
        
        ExprNodeDesc expNode = Utilities.deserializeExpression(exp1xml, conf);
        walkNode(expNode, 0);
        assertNotNull(expNode);
        
        System.out.println("after parse");
        
        ExprNodeDesc newExpNode = NodeProcessor.getNodeProcessor(expNode).parseNode(expNode, null, 0);
        walkNode(newExpNode, 0);
        assertNotNull(newExpNode);
    }
    
    public void testParseDriver() {
        String command = "select * from alog.d_20141110 where hour = 10 ";
        
        HiveConf conf = new HiveConf();
        Context ctx;
        try {
            ctx = new Context(conf);
            ctx.setTryCount(1);
            ctx.setCmd(command);
            ctx.setHDFSCleanup(true);
            
            ParseDriver pd = new ParseDriver();
            ASTNode tree = pd.parse(command, ctx);
            System.out.println(tree.dump());
            
            BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
            
            sem.analyze(tree, ctx);
            
            if (sem instanceof SemanticAnalyzer) {
                SemanticAnalyzer newsem = (SemanticAnalyzer) sem;
                //newsem.genExprNodeDesc(expr, input);
            }
            
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SemanticException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    @Test
    public void testDeserialized2() {
        Configuration conf = new Configuration();
        
        ExprNodeDesc expNode = Utilities.deserializeExpression(exp2xml, conf);
        walkNode(expNode, 0);
        assertNotNull(expNode);
        
        System.out.println("after parse");
        
        ExprNodeDesc newExpNode = NodeProcessor.getNodeProcessor(expNode).parseNode(expNode, null, 0);
        walkNode(newExpNode, 0);
        assertNotNull(newExpNode);
        
        System.out.println(newExpNode.getExprString());
    }
    
    
    @Test
    public void testDeserialized3() {
        
        System.out.println(" exprnodedesc test 3");
        Configuration conf = new Configuration();
        
        ExprNodeDesc expNode = Utilities.deserializeExpression(exp3xml, conf);
        walkNode(expNode, 0);
        assertNotNull(expNode);
        
        System.out.println("after parse");
        
        ExprNodeDesc newExpNode = NodeProcessor.getNodeProcessor(expNode).parseNode(expNode, null, 0);
        walkNode(newExpNode, 0);
        assertNotNull(newExpNode);
        
        System.out.println("final condition string \n" + newExpNode.getExprString());
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
