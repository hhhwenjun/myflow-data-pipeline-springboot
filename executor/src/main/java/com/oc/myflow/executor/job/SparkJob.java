package com.oc.myflow.executor.job;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SparkJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(SparkJob.class);
    // initialize spark launcher, read it from your config file
    @Value("${sparkHome}")
    //echo 'sc.getConf.get("spark.home")' | spark-shell
    private String sparkHome;
    private SparkAppHandle handler;


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        long startTime = System.currentTimeMillis(); // calculate execution time

        JobDataMap map = context.getMergedJobDataMap();
        appLogger.info("Begin to run " + map.getString("taskName") + " " + map.getString("stepName"));
        try {
            // status of running log
            Boolean isSubmitLogDisplay = false;
            Boolean isRunningLogDisplay = false;
            Boolean isFailedLogDisplay = false;
            Boolean isUnknowLogDisplay = false;
            Boolean isKilledLogDisplay = false;
            Boolean isConnectLogDisplay = false;
            Boolean isLostLogDisplay = false;

            SparkLauncher sparkLauncher = new SparkLauncher()
                    .setAppResource(map.getString("path"))
                    .setMainClass(map.getString("className"))
                    .setMaster(map.getString("master"))
                    .setSparkHome(sparkHome)
                    .setDeployMode(map.getString("deployMode"))
//                    .addFile()
//                    .addAppArgs("123","456") // input of the file
//                            .addSparkArg("--queue",map.getString("queue")) // for spark running
                    ;
            // jar is uploaded to servers, also upload your config
            // otherwise cannot find some files since not in local machine
            List<String> confList = null;
            if (map.get("conf") != null) {
                confList = (List<String>) map.get("conf");
            }
            List<String> sparkArgList = null;
            if (map.get("sparkArg") != null) {
                sparkArgList = (List<String>) map.get("sparkArg");
            }
            List<String> appArgList = null;
            if (map.get("appArgs") != null) {
                appArgList = (List<String>) map.get("appArgs");
            }

            if (map.getString("propertiesFile") != null) {
                sparkLauncher.setPropertiesFile(map.getString("propertiesFile"));
            }
            // other supporting files, such as csv
            if (map.get("files") != null) {
                List<String> fileList = (List<String>) map.get("files");
                for (String file : fileList) {
                    sparkLauncher.addFile(file);
                }
            }

            if (confList != null) {
                for (String conf : confList) {
                    String confName = conf.substring(0, conf.indexOf("="));
                    String confValue = conf.substring(conf.indexOf("=") + 1);
                    sparkLauncher.setConf(confName, confValue);
                }
            }
            if (appArgList != null) {
                String[] appArgs = appArgList.stream().toArray(String[]::new);
                sparkLauncher.addAppArgs(appArgs);
            }
            if (sparkArgList != null) {
                for (String sparkArg : sparkArgList) {
                    String sparkArgName = sparkArg.substring(0, sparkArg.indexOf('='));
                    String sparkArgValue = sparkArg.substring(sparkArg.indexOf('=') + 1);
                    if (sparkArgName.startsWith("--")) {
                        sparkLauncher.addSparkArg(sparkArgName, sparkArgValue);
                    } else {
                        sparkLauncher.addSparkArg("--" + sparkArgName, sparkArgValue);
                    }
                }
            }
            handler = sparkLauncher.redirectOutput(new File(map.getString("sparkLogPath") + "/Task_" + startTime + ".log")).startApplication();
            appLogger.info("Task start, run main class: " + map.getString("className"));

            // reduce repeated logs, only show log once when reach the status
            while (handler.getState() != null && !handler.getState().isFinal()) {
                if (handler.getState().equals(SparkAppHandle.State.SUBMITTED) && !isSubmitLogDisplay) {
                    appLogger.info("Task" + startTime + " is SUBMITTED Application Id is " + handler.getAppId());
                    isSubmitLogDisplay = true;
                } else if (handler.getState().equals(SparkAppHandle.State.RUNNING) && !isRunningLogDisplay) {
                    appLogger.info("Task" + startTime + " is RUNNING Application Id is " + handler.getAppId());
                    isRunningLogDisplay = true;
                } else if (handler.getState().equals(SparkAppHandle.State.FAILED) && !isFailedLogDisplay) {
                    appLogger.info("Task" + startTime + " is FAILED Application Id is " + handler.getAppId());
                    isFailedLogDisplay = true;
                } else if (handler.getState().equals(SparkAppHandle.State.KILLED) && !isKilledLogDisplay) {
                    appLogger.info("Task" + startTime + " is KILLED Application Id is " + handler.getAppId());
                    isKilledLogDisplay = true;
                } else if (handler.getState().equals(SparkAppHandle.State.UNKNOWN) && !isUnknowLogDisplay) {
                    appLogger.info("Task" + startTime + " is UNKNOWN Application Id is " + handler.getAppId());
                    isUnknowLogDisplay = true;
                } else if (handler.getState().equals(SparkAppHandle.State.CONNECTED) && !isConnectLogDisplay) {
                    appLogger.info("Task" + startTime + " is CONNECTED Application Id is " + handler.getAppId());
                    isConnectLogDisplay = true;
                } else if (handler.getState().equals(SparkAppHandle.State.LOST) && !isLostLogDisplay) {
                    appLogger.info("Task" + startTime + " is LOST Application Id is " + handler.getAppId());
                    isLostLogDisplay = true;
                }
            }
            if (handler.getState().equals(SparkAppHandle.State.KILLED)) {
                appLogger.info("Task" + startTime + " is KILLED Application Id is " + handler.getAppId());
            }
            if (handler.getState().equals(SparkAppHandle.State.FAILED)) {
                appLogger.info("Task" + startTime + " is FAILED Application Id is " + handler.getAppId());
            }
            appLogger.info("Task is done");
        } catch (Exception e) {
            appLogger.error("Execute Script Error:", e);
        } finally {
            long endTime = System.currentTimeMillis();
            appLogger.info("Finish run " + map.getString("taskName") + " " + map.getString("stepName"));
        }
    }
}
