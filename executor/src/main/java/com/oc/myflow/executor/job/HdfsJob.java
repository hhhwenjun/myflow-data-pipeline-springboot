package com.oc.myflow.executor.job;

import com.oc.myflow.common.utils.CmdUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.net.URI;


public class HdfsJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(HdfsJob.class);
    @Value("${coreSite}")
    private String coreSite;
    @Value("${hdfsSite}")
    private String hdfsSite;
    @Value("${hadoopNode}")
    private String hadoopNode;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        appLogger.info("Begin to run HDFS job");
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        try {
            Configuration conf = new Configuration(); // help find hadoop configuration, spark etc
            conf.addResource(new Path(coreSite));
            conf.addResource(new Path(hdfsSite));
            String mode = paramMap.getString("mode");
            // hadoop root node, hadoop# hive - show table location
            // fs is hadoop file system
            // hdfs://localhost:8020
            FileSystem fs = FileSystem.get(new URI(hadoopNode), conf);

            if (mode.equalsIgnoreCase("copyFromLocal")){
                String srcPath = paramMap.getString("source");
                String destPath = paramMap.getString("destination");
                fs.copyFromLocalFile(new Path(srcPath), new Path(destPath));

            } else if (mode.equalsIgnoreCase("copyToLocal")){
                String srcPath = paramMap.getString("source");
                String destPath = paramMap.getString("destination");
                fs.copyToLocalFile(new Path(srcPath), new Path(destPath));

            } else {
                appLogger.warn("Mode " + mode + " not be implemented yet");
            }
            appLogger.info("HDFS job is done");
        } catch (Exception e) {
            appLogger.error("Error:", e);
        }
    }
}
