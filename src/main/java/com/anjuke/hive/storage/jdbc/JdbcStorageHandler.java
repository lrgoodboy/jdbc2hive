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
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        HiveConfiguration.copyJDBCProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        HiveConfiguration.copyJDBCProperties(tableDesc, jobProperties);
    }

    @Override
    @Deprecated
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        HiveConfiguration.copyJDBCProperties(tableDesc, jobProperties);
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
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
        return JdbcSerDe.class;
    }

}
