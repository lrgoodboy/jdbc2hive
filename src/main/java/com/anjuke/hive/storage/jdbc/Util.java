package com.anjuke.hive.storage.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;

/**
 * 
 * read data from hive configuration.
 * 
 * use static Util to simple it.
 * 
 * @author zhiwensun
 *
 */
public class Util {
    
    public static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

}
