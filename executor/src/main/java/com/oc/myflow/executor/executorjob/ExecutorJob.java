package com.oc.myflow.executor.executorjob;

import com.oc.myflow.executor.job.*;
import com.oc.myflow.executor.listener.OrderListener;
import com.oc.myflow.model.scheduler.JobDescriptor;
import com.oc.myflow.model.vo.StepVO;
import com.oc.myflow.model.vo.TaskVO;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

public class ExecutorJob implements Job {
    @Autowired
    private Scheduler scheduler;

    private static final Logger appLogger = LoggerFactory.getLogger(ExecutorJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        String taskName = paramMap.getString("taskName");
        String taskId = paramMap.getString("taskId");
        String group = paramMap.getString("group");
        TaskVO taskVO = (TaskVO)paramMap.get("taskVO");
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
        Map<String, String> statusMap = new HashMap<>();
        taskVO.getSteps().forEach(stepVO -> {
            appLogger.info("Get step" + stepVO.getOrder() + "Config");
            statusMap.put(stepVO.getStepName(), "new");
            String type = stepVO.getType();
            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(group);
            jobDescriptor.setName(taskName);
            jobDescriptor.setName(stepVO.getStepName());
            paramMap.put("order", stepVO.getOrder());
            paramMap.put("stepName", stepVO.getStepName());
            if (type.equalsIgnoreCase("hive")) {
                // connect to hive database, usually each company has only 1
                // only 1 configuration
                // run sql command in your hive database
                // put the JSON path
                // Java reflection: any object inherit the job class, no need to know which class
                jobDescriptor.setJobClazz(HiveJob.class);
                paramMap.put("path", stepVO.getPath());
                paramMap.put("hiveParam", stepVO.getHiveParam());
            } else if (type.equalsIgnoreCase("script")){
                jobDescriptor.setJobClazz(ScriptJob.class);
                paramMap.put("path", stepVO.getPath());
                paramMap.put("param", stepVO.getParam());
                paramMap.put("mode", stepVO.getMode());
            }
            else if (type.equalsIgnoreCase("spark")){
                jobDescriptor.setJobClazz(SparkJob.class);
                paramMap.put("path", stepVO.getPath());
                paramMap.put("master", stepVO.getMaster());
                paramMap.put("deployMode", stepVO.getDeployMode());
                paramMap.put("className", stepVO.getClassName());
                paramMap.put("sparkLogPath", stepVO.getSparkLogPath());
            }
            else if (type.equalsIgnoreCase("HDFS")){
                jobDescriptor.setJobClazz(HdfsJob.class);
                paramMap.put("mode", stepVO.getMode()); // location transport
                paramMap.put("source", stepVO.getSource());
                paramMap.put("destination", stepVO.getDestination());
            }
            else if (type.equalsIgnoreCase("function")){
                jobDescriptor.setJobClazz(FuncJob.class);
                paramMap.put("path", stepVO.getPath());
                paramMap.put("className", stepVO.getClassName());
                paramMap.put("methodName", stepVO.getMethodName());
                Map<String, String> funcParamMap = new HashMap<>();
                if (stepVO.getFuncParamList() != null){
                    stepVO.getFuncParamList().forEach(str -> {
                        String[] tempArr = str.split("=");
                        funcParamMap.put(tempArr[0], tempArr[1]);
                    });
                    paramMap.put("funcParamMap", funcParamMap);
                }
            }
            jobDescriptor.setDataMap(paramMap);
            JobDetail jobDetail = jobDescriptor.buildJobDetail();
            jobDetailQueue.add(jobDetail);
        });
        List<String> runningJobList = new ArrayList<>();
        orderListener.setJobDetailQueue(jobDetailQueue);
        orderListener.setStatusMap(statusMap);
        orderListener.setRunningJobList(runningJobList);

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
                String stepName = initJobDetail.getJobDataMap().
                        getString("stepName");
                runningJobList.add(stepName);
                statusMap.put(stepName, "processing");
                //  scheduler.addJob(initJobDetail, true);
                appLogger.info(stepName + " is running");
                scheduler.addJob(initJobDetail, true);
                scheduler.triggerJob(initJobDetail.getKey());
            }
        } catch (Exception e) {
            appLogger.error("Error", e);
        }
    }
}
