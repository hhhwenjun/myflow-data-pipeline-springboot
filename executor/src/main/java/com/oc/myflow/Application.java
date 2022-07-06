package com.oc.myflow;

import com.oc.myflow.common.ConfigUtil;
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

            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(taskName);

            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("taskName", taskName);
            paramMap.put("taskId", taskId);
            paramMap.put("cron", cron);
            jobDescriptor.setDataMap(paramMap);
            OrderListener orderListener = new OrderListener(taskName + "OrderListener");

            /* trigger job requires job key and job map,
             * simple trigger, usually in test
             * scheduler.triggerJob();
             */
            //scheduler.scheduleJob(); // requires job detail
            appLogger.info("Get task" + taskId + "Config");
            // Under the step level
            taskVO.getSteps().sort(new Comparator<StepVO>() {
                @Override
                public int compare(StepVO o1, StepVO o2) {
                    return Integer.parseInt(o1.getOrder()) - Integer.parseInt(o2.getOrder());
                }
            });
            Queue<JobDetail> jobDetailQueue = new LinkedList<>();
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
                    paramMap.put("order", stepVO.getOrder());
                    paramMap.put("stepName", stepVO.getStepName());
                    paramMap.put("path", stepVO.getPath());
                    paramMap.put("hiveParam", stepVO.getHiveParam());
                } else if (type.equals("spark")){
                    // jobDescriptor.setJobClazz(SparkJob.class);
                }
                JobDetail jobDetail = jobDescriptor.buildJobDetail();
                jobDetailQueue.add(jobDetail);
            });
            orderListener.setJobDetailQueue(jobDetailQueue);

            // after creating the job steps, put them into the schedule
            Trigger jobTrigger = TriggerBuilder.newTrigger()
                    .withIdentity(taskName)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build();
            try {
                // use a queue to store the tasks and maintain the task order
                // only trigger the first task and let it trigger the rest
                scheduler.getListenerManager().addJobListener(orderListener);
                if (jobDetailQueue.isEmpty()) {
                    appLogger.warn(taskName + "'s Step List is empty");
                    return;
                }
                while (!jobDetailQueue.isEmpty() &&
                        jobDetailQueue.peek().getJobDataMap().getString("order").equals("1")){
                    JobDetail initJobDetail = jobDetailQueue.poll();
                    scheduler.addJob(initJobDetail, true);
                }
                scheduler.scheduleJob(jobTrigger);
            } catch (Exception e) {
                appLogger.error("Error", e);
            }
        });
    }
}
