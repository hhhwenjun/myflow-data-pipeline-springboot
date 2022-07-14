package com.oc.myflow.executor.job;


import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.net.URL;

public class FuncJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(FuncJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        appLogger.info("Begin to run Function job");
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        try {

            // java reflection
            URLClassLoader child = new URLClassLoader(new URL[]{
                    new File(paramMap.getString("path")).toURI().toURL()},
                    this.getClass().getClassLoader());
            appLogger.info("class: " + paramMap.getString("className"));
            appLogger.info("method: " + paramMap.getString("methodName"));

            Class classToLoad = Class.forName(paramMap.getString("className"),
                    true, child);
            Method method = classToLoad.getDeclaredMethod(
                    paramMap.getString("function"), JobDataMap.class);
            method.setAccessible(true); // allow for visit
            Object instance = classToLoad.newInstance();
            method.invoke(instance, paramMap);

            appLogger.info("Function job is done");
        } catch (Exception e) {
            appLogger.error("Error:", e);
        }
    }
}
