package com.anjuke.hive.storage;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Before;
import org.junit.Test;

import com.anjuke.hive.storage.db.Dao;
import com.anjuke.hive.storage.db.DaoFactory;
import com.anjuke.hive.storage.db.MySQLDao;
import com.anjuke.hive.storage.jdbc.Bound;
import com.anjuke.hive.storage.jdbc.HiveConfiguration;
import com.anjuke.hive.storage.jdbc.JdbcInputSplit;
import com.anjuke.hive.storage.splitter.LongSplitter;

public class DaoTest {
    
    private Dao dao;
    
    @Before
    public void initDao() {
        Configuration conf = new Configuration();
        conf.addResource(DaoTest.class.getClassLoader().getResourceAsStream("jdbc.xml"));
        dao = DaoFactory.getDao(conf);
    }
    
    @Test
    public void testDao() {
        assertNotNull(dao);
    }
    
    @Test
    public void testHive() {
        PrimitiveCategory category;
        
        category = PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName("int").primitiveCategory;
        switch (category) {
        case INT :
            System.out.println("int found ");
            break;
        default:
            break;
        }
        
    }
    
    @Test
    public void testGetRowDataLength() {
        dao.setTableName("q1");
        dao.setSelectFields(Arrays.asList(new String[]{"id"}));
        
        int rowlen = dao.getRowDataLength();
        
        assertTrue(rowlen > 0);
        assertTrue(rowlen == 4);
        
        dao.setTableName("place_info");
        dao.setSelectFields(Arrays.asList(new String[]{"id", "lng", "lat", "geocode", "place_info_id", "type"}));
        
        rowlen = dao.getRowDataLength();
        assertTrue(rowlen > 0);
    }
    
    @Test
    public void testGetAffetcedRows() {
        dao.setTableName("place_info");
        dao.setSelectFields(Arrays.asList(new String[]{"id", "lng", "lat", "geocode", "place_info_id", "type"}));
        
        long affectedRows = dao.getAffetcedRows();
        assertTrue(affectedRows > 0);
    }
    
    @Test
    public void testSplitter() {
        dao.setTableName("place_info");
        dao.setSelectFields(Arrays.asList(new String[]{"id", "lng", "lat", "geocode", "place_info_id", "type"}));
        
        long affectedRows = dao.getAffetcedRows();
        
        LongSplitter splitter = new LongSplitter();
        Bound bound = new Bound("id");
        //bound.setUpper(1372687);
        //bound.setLower(1);
        
        bound = dao.getConditionBound(new Bound("id"));
        
        List<JdbcInputSplit> splits = splitter.getSplits(affectedRows, dao.getRowDataLength(), bound, 10 * 1024 * 1024, new Path[]{new Path("/tmp/jdbc2hivetest2")});
        for (JdbcInputSplit split : splits) {
            //System.out.println(split.getLowerCause() + " and " + split.getUpperCause());
            dao.setSplit(split);
            System.out.println(((MySQLDao)dao).getSplitCondition());
        }
        
    }

}
