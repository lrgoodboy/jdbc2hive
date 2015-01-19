package com.anjuke.hive.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;

import com.anjuke.hive.storage.jdbc.HiveConfiguration;
import com.anjuke.hive.storage.jdbc.JdbcInputFormat;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;

public class JdbcInputFormatTest {
    
    private JobConf conf = new JobConf();
    
    @Before
    public void initConf() {
        conf.addResource(Thread.currentThread().getContextClassLoader().getResourceAsStream("jdbc.xml"));
    }
    
    @Test
    public void testInput() throws IOException {
        conf.setInt("dfs.blocksize", 1* 1024*1024);
        
        conf.set(org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,1,2");
        conf.set(serdeConstants.LIST_COLUMNS, "hive_id, hive_lng, hive_lat");
        conf.set(serdeConstants.LIST_COLUMN_TYPES, "int:int:int");
        conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, readFile("expnode4.xml"));
        
        conf.set(HiveConfiguration.COLUMN_MAP, "hive_id=id, hive_lng=lng, hive_lat=lat");
        
        JdbcInputFormat inputFormat = new JdbcInputFormat();
        
        InputSplit[]  splits = inputFormat.getSplits(conf, 1);
        
        JdbcInputSplit jdbcinput = null;
        for (InputSplit split: splits) {
            jdbcinput = (JdbcInputSplit) split;
            System.out.println(jdbcinput.getLowerCause() + " and " + jdbcinput.getUpperCause());
        }
        
        RecordReader<LongWritable, MapWritable>  rr = inputFormat.getRecordReader(jdbcinput, conf, null);
        LongWritable key = rr.createKey();
        MapWritable value = rr.createValue();
        
        while(rr.next(key, value)) {
            System.out.println(key.toString());
            for (Entry<Writable, Writable> entry : value.entrySet()) {
                System.out.println("\t" + entry.getKey() + " = " + entry.getValue());
            }
        }
    }
    
    
    private String readFile(String fileName) {
        
        InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        BufferedReader bf;
        String result = "", tmp=null;
        
        try {
            bf = new BufferedReader(new InputStreamReader(ins, "utf-8"));
            while ((tmp = bf.readLine()) != null) {
                result += tmp;
            }
            bf.close();
            ins.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return result;
    }

}
