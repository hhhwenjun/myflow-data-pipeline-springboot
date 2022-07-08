package com.oc.myflow.executor.listener;

import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.listeners.JobChainingJobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class OrderListener extends JobChainingJobListener {

    private static final Logger appLogger = LoggerFactory.getLogger(OrderListener.class);
    private Queue<JobDetail> jobDetailQueue;
    private List<String> runningJobList;

    public List<String> getRunningJobList() {
        return runningJobList;
    }

    public void setRunningJobList(List<String> runningJobList) {
        this.runningJobList = runningJobList;
    }

    public Map<String, String> getStatusMap() {
        return statusMap;
    }

    public void setStatusMap(Map<String, String> statusMap) {
        this.statusMap = statusMap;
    }

    private Map<String, String> statusMap;

    public OrderListener(String name){
        super(name);
    }

    public void setJobDetailQueue(Queue<JobDetail> jobDetailQueue){
        this.jobDetailQueue = jobDetailQueue;
    }

    public Queue<JobDetail> getJobDetailQueue(){
        return jobDetailQueue;
    }

    @Override
    public void jobWasExecuted(JobExecutionContext jobExecutionContext, JobExecutionException e) {
        JobDetail currJobDetail = jobExecutionContext.getJobDetail();
        String curStepName = currJobDetail.getJobDataMap().getString("stepName");
        if (curStepName == null) return;
        statusMap.put(currJobDetail.getJobDataMap().getString("stepName"), "complete");
        appLogger.info(currJobDetail.getJobDataMap().getString("stepName") + "'s is done");
        runningJobList.remove(curStepName);
        // parallel computing check if still steps need to be done before moving forward
        if (runningJobList.isEmpty()){
            try{
                if (!jobDetailQueue.isEmpty()){
                    JobDetail nextJobDetail = jobDetailQueue.peek();
                    String nextOrder = nextJobDetail.getJobDataMap().getString("order");
                    while (!jobDetailQueue.isEmpty() &&
                            jobDetailQueue.peek().getJobDataMap().getString("order").equals(nextOrder)){
                        nextJobDetail = jobDetailQueue.poll();
                        String nextStepName = nextJobDetail.getJobDataMap().getString("stepName");
                        appLogger.info(nextStepName+ "is running");
                        jobExecutionContext.getScheduler().addJob(nextJobDetail, true);
                        runningJobList.add(nextStepName);
                        jobExecutionContext.getScheduler().triggerJob(nextJobDetail.getKey());
                    }
                }
                else {
                    appLogger.info("All jobs are done");
                }
            } catch (Exception ex){
                appLogger.error("Error: ", ex);
            }
        }
        else {
            appLogger.info("Waiting for other steps to be done.");
        }
    }
}
