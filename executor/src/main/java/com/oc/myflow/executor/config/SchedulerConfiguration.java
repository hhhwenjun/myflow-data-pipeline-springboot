package com.oc.myflow.executor.config;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;
import org.springframework.util.Assert;

/**
 * Quartz Configuration
 *
 * Create new scheduler and spring helps you do configuration
 */
@Configuration
public class SchedulerConfiguration {
    /**
     * Provide the control of the scheduler to Spring
     */
    public class AutowireCapableBeanJobFactory extends SpringBeanJobFactory{
        private final AutowireCapableBeanFactory beanFactory;

        @Autowired
        public AutowireCapableBeanJobFactory(AutowireCapableBeanFactory beanJobFactory) {
            Assert.notNull(beanJobFactory,"Bean factory must not be null");
            this.beanFactory = beanJobFactory;
        }

        @Override
        protected Object createJobInstance(TriggerFiredBundle bundle) throws Exception{
            Object jobInstance=super.createJobInstance(bundle);
            this.beanFactory.autowireBean(jobInstance);
            this.beanFactory.initializeBean(jobInstance,"jobInstance");
            return jobInstance;
        }
    }

    @Bean
    public SchedulerFactoryBean schedulerFactory(ApplicationContext applicationContext){
        SchedulerFactoryBean schedulerFactoryBean=new SchedulerFactoryBean();
        schedulerFactoryBean.setJobFactory(new AutowireCapableBeanJobFactory(applicationContext.getAutowireCapableBeanFactory()));
        return schedulerFactoryBean;
    }

    @Bean
    public Scheduler scheduler(ApplicationContext applicationContext) throws SchedulerException {
        Scheduler scheduler=schedulerFactory(applicationContext).getScheduler();
        scheduler.start();
        return scheduler;
    }
}
