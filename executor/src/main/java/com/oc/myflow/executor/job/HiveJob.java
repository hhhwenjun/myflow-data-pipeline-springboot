package com.oc.myflow.executor.job;

import com.oc.myflow.Application;
import com.oc.myflow.executor.service.impl.HiveServiceImpl;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public class HiveJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(HiveJob.class);
    @Autowired
    private HiveServiceImpl hiveService;
    /**
     * Let Quartz control the running Hql process
     * @param jobExecutionContext
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        appLogger.info("Run Hive");
        hiveService.runHql(paramMap.getString("path"), (Map<String, Object>)paramMap.get("hiveParam"));
    }
}
