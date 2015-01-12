package com.anjuke.hive.storage.jdbc;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

public class JdbcOutputFormat implements OutputFormat<NullWritable, MapWritable>,
HiveOutputFormat<NullWritable, MapWritable> {

    @Override
    public RecordWriter getHiveRecordWriter(JobConf arg0, Path arg1,
            Class<? extends Writable> arg2, boolean arg3, Properties arg4,
            Progressable arg5) throws IOException {
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
            throws IOException {
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(
            FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
            throws IOException {
        return null;
    }

}
