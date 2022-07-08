package com.oc.myflow;

import com.oc.myflow.common.ConfigUtil;
import com.oc.myflow.executor.executorjob.ExecutorJob;
import com.oc.myflow.executor.job.HiveJob;
import com.oc.myflow.executor.listener.OrderListener;
import com.oc.myflow.executor.service.impl.HiveServiceImpl;
import com.oc.myflow.model.scheduler.JobDescriptor;
import com.oc.myflow.model.vo.ConfigVO;
import com.oc.myflow.model.vo.StepVO;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.util.*;


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
            jobDescriptor.setDataMap(paramMap);
            JobDetail executorJobDetail = jobDescriptor.buildJobDetail();

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
}
