package com.oc.myflow.executor.job;

import com.oc.myflow.common.utils.CmdUtil;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class ScriptJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(ScriptJob.class);
    @Autowired
    private CmdUtil cmdUtil;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        appLogger.info("Begin to run script job");
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        try {
            String filePath = paramMap.getString("path");
            String mode = paramMap.getString("mode");
            if (filePath == null || "".equals(filePath.trim()))
                throw new Exception("Require Path attribute in script type json config");
            String scriptParams = paramMap.getString("param");
            if ("python2".equals(mode)) {
                cmdUtil.executeCommand("python2 -u " + filePath, scriptParams);
            } else if ("python3".equals(mode)) {
                cmdUtil.executeCommand("python3 -u " + filePath, scriptParams);
            } else {
                cmdUtil.executeCommand(filePath, scriptParams);
            }
            appLogger.info("Script job is done");
        } catch (Exception e) {
            appLogger.error("Error:", e);
        }
    }
}
