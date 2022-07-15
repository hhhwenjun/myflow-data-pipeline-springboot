package com.oc.myflow;

import com.oc.myflow.common.utils.ConfigUtil;
import com.oc.myflow.executor.executorjob.ExecutorJob;
import com.oc.myflow.model.scheduler.JobDescriptor;
import com.oc.myflow.model.vo.ConfigVO;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.sql.Driver;
import java.util.*;


@SpringBootApplication
public class Application {
    @Autowired
    private ConfigUtil configUtil;

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private JdbcTemplate hiveJdbcTemplate; // jdbc, run sql

    @Autowired
    private HiveTemplate hiveTemplate; // run hql

    @Value("${jsonConfigPath}")
    private String jsonConfigPath;

    private static final Logger appLogger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @PostConstruct
    public void run() throws FileNotFoundException {
        appLogger.info("MyFlow begin running");
        ConfigVO configVO = configUtil.getConfigVO();
        // show out when there is a bug
        appLogger.debug("Get ConfigVO");
        List<ConfigVO.DataSource> dataSources = configVO.getDataSource();
        appLogger.info("Get Data Source Config");
        configVO.getTasks().forEach(taskVO -> {
            String taskId = taskVO.getTaskId();
            String taskName = taskVO.getTaskName();
            String cron = taskVO.getCron();
            String group = taskVO.getGroup();

            // create a job descriptor
            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(group);
            jobDescriptor.setName(taskName);
            jobDescriptor.setJobClazz(ExecutorJob.class);
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("taskName", taskName);
            paramMap.put("taskId", taskId);
            paramMap.put("cron", cron);
            paramMap.put("taskVO", taskVO);
            paramMap.put("group", group);
            paramMap.put("dataSourceMap", getDataSourceMap(dataSources));

            jobDescriptor.setDataMap(paramMap);
            JobDetail executorJobDetail = jobDescriptor.buildJobDetail();

            // If require other operations between jobs, use function oo programming(new project, new jar)
            // create a function class, input: paramMap, the output store in paramMap
            // and get the result from the same paramMap

            // after creating the job steps, put them into the schedule
            Trigger jobTrigger = TriggerBuilder.newTrigger()
                    .withIdentity(taskName, group)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build();

            try {
                scheduler.scheduleJob(executorJobDetail, jobTrigger);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }

        });
    }

    public Map<String, Object> getDataSourceMap(List<ConfigVO.DataSource> dataSources){
        Map<String, Object> dataSourceMap = new HashMap<>();

        dataSources.forEach(dataSource -> {
            if (dataSource.getName().equals("hive")){
                dataSourceMap.put("hive", hiveJdbcTemplate);
                dataSourceMap.put("hiveTemplate", hiveTemplate);
            }
            else {
                // create a data source
                SimpleDriverDataSource ds = new SimpleDriverDataSource();
                Class<?> cls = null;
                try {
                    // provide your data source driver
                    cls = Class.forName(dataSource.getDriver());
                    ds.setDriverClass((Class<? extends Driver>) cls);
                    ds.setUrl(dataSource.getUrl());
                    ds.setUsername(dataSource.getUserName());
                    ds.setPassword(dataSource.getPassword());

                    // create jdbc template
                    JdbcTemplate jtm = new JdbcTemplate(ds);
                    dataSourceMap.put(dataSource.getName(), jtm);

                } catch (Exception e){
                    appLogger.error("Error", e);
                }
            }
        });
        return dataSourceMap;
    }
}
