package com.anjuke.hive.storage.jdbc;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class JdbcStorageHandler implements HiveStorageHandler {
    
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void configureInputJobProperties(TableDesc arg0,
            Map<String, String> arg1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void configureOutputJobProperties(TableDesc arg0,
            Map<String, String> arg1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    @Deprecated
    public void configureTableJobProperties(TableDesc arg0,
            Map<String, String> arg1) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        // TODO Auto-generated method stub
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return JdbcInputFormat.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return JdbcOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return null;
    }

}
