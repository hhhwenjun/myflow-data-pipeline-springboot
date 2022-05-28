package com.oc.myflow;

import com.oc.myflow.common.ConfigUtil;
import com.oc.myflow.executor.job.HiveJob;
import com.oc.myflow.executor.service.impl.HiveServiceImpl;
import com.oc.myflow.model.scheduler.JobDescriptor;
import com.oc.myflow.model.vo.ConfigVO;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SpringBootApplication
public class Application {
    @Autowired
    private ConfigUtil configUtil;

    @Autowired
    private Scheduler scheduler;

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

            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(taskName);

            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("taskName", taskName);
            paramMap.put("taskId", taskId);
            paramMap.put("cron", cron);
            jobDescriptor.setDataMap(paramMap);

            /* trigger job requires job key and job map,
             * simple trigger, usually in test
             * scheduler.triggerJob();
             */
            //scheduler.scheduleJob(); // requires job detail
            appLogger.info("Get task" + taskId + "Config");
            // Under the step level
            taskVO.getSteps().forEach(stepVO -> {
                appLogger.info("Get step" + stepVO.getOrder() + "Config");
                String type = stepVO.getType();
                jobDescriptor.setName(stepVO.getStepName());
                if (type.equals("hive")) {
                    // connect to hive database, usually each company has only 1
                    // only 1 configuration
                    // run sql command in your hive database
                    // put the JSON path
                    // Java reflection: any object inherit the job class, no need to know which class
                    jobDescriptor.setJobClazz(HiveJob.class);
                    paramMap.put("path", stepVO.getPath());
                    paramMap.put("hiveParam", stepVO.getHiveParam());
                } else if (type.equals("spark")){
                    // jobDescriptor.setJobClazz(SparkJob.class);
                }
                JobDetail jobDetail = jobDescriptor.buildJobDetail();
                // after creating the job steps, put them into the schedule
                Trigger jobTrigger = TriggerBuilder.newTrigger()
                        .withIdentity(stepVO.getStepName())
                        .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                        .build();
                try {
                    scheduler.scheduleJob(jobDetail, jobTrigger);
                } catch (SchedulerException e) {
                    appLogger.error("Error", e);
                }
            });

        });
    }
}
