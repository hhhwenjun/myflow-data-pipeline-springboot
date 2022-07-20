package com.oc.myflow.executor.job;


import com.oc.myflow.executor.handler.DataTransferHandler;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataloaderJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(DataloaderJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        appLogger.info("Begin to run Dataloader job");
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        Map<String, Object> dataSourceMap = (Map<String, Object>) paramMap.get("dataSourceMap");
        try {
            // move data from source to dest
            JdbcTemplate sourceJtm = (JdbcTemplate) paramMap.get(
                    paramMap.getString("sourceDataSource"));
            JdbcTemplate destJtm = (JdbcTemplate) paramMap.get(
                    paramMap.getString("destDataSource"));

            // source tables should match dest tables (at least same size)
            // in real tables, should bring your schema (sourceTable.employee)
            List<String> sourceTables = (List<String>) paramMap.get("sourceTables");
            List<String> destTables = (List<String>) paramMap.get("destTables");
            if (sourceTables == null || destTables == null ||
                    sourceTables.isEmpty() || destTables.isEmpty() ||
                    sourceTables.size() != destTables.size()){
                throw new Exception("The table list and dest table list must have value " +
                        "and same size");
            }

            // load data from source to dest
            for (int i = 0; i < sourceTables.size(); i++){
                String destTable = destTables.get(i);
                String sourceTable = sourceTables.get(i);
                appLogger.info("Being to load data from " + sourceTable + " to " + destTable);
                // If need to do some other operations to data, create script and use script job

                // id name age
                // 1 jake 22
                appLogger.info("select * from " + sourceTable);
                DataTransferHandler dataTransferHandler =
                        new DataTransferHandler(destJtm, destTable);

                // for big data, each time read a line (or 10/100 lines)
                // need a rowback handler
                sourceJtm.query("select * from " + sourceTable, dataTransferHandler);
                dataTransferHandler.saveRest();
                // select * from can only be used for a small amount of data
                // if the table is too large, ram is not enough

                appLogger.info("Load data from " + sourceTable + " to " + destTable + "complete");
            }
            appLogger.info("Dataloader job is done");
        } catch (Exception e) {
            appLogger.error("Error:", e);
        }
    }
}
