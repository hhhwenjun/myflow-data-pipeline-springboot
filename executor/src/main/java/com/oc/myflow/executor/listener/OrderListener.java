package com.oc.myflow.executor.listener;

import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.listeners.JobChainingJobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class OrderListener extends JobChainingJobListener {

    private static final Logger appLogger = LoggerFactory.getLogger(OrderListener.class);
    private Queue<JobDetail> jobDetailQueue;

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
        appLogger.info(currJobDetail.getJobDataMap().getString("stepName") + "'s is done");
        try{
            // process tasks in order, no parallel computing
            if (!jobDetailQueue.isEmpty()){
                JobDetail nextJobDetail = jobDetailQueue.poll();
                appLogger.info(nextJobDetail.getJobDataMap().getString("stepName")+ "'s is done");
                jobExecutionContext.getScheduler().addJob(nextJobDetail, true);
                jobExecutionContext.getScheduler().triggerJob(nextJobDetail.getKey());
            }
            else {
                appLogger.info("All jobs are done");
            }

        } catch (Exception ex){
            appLogger.error("Error: ", ex);

        }

    }
}
