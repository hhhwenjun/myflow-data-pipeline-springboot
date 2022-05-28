package com.oc.myflow.model.scheduler;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;

import java.util.HashMap;
import java.util.Map;

import static org.quartz.JobBuilder.newJob;

public class JobDescriptor {
    private String name;
    private String group;
    /*
     * unknown job type -> object, job map, job key + job
     * have to have a job data map, so we can use 'new' here
     */
    private Map<String, Object> dataMap = new HashMap<>();
    private boolean durable = true;
    /*
     * class is any class that inherited from quartz.job
     * do not directly use 'class', use 'clazz' instead
     */
    private Class<? extends org.quartz.Job> jobClazz;

    public Class<? extends Job> getJobClazz() {
        return jobClazz;
    }

    public void setJobClazz(Class<? extends Job> jobClazz) {
        this.jobClazz = jobClazz;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Map<String, Object> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public JobDetail buildJobDetail(){
        // task durable: keep the task info and status in file
        JobDataMap jobDataMap = new JobDataMap(getDataMap());
        // a task is a group of steps, input the quartz params
        return newJob(jobClazz)
                .withIdentity(getName(), getGroup())
                .usingJobData(jobDataMap)
                .storeDurably(isDurable())
                .build();
    }
}
