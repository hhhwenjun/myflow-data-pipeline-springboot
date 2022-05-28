package com.oc.myflow.executor.service.impl;

import com.oc.myflow.Application;
import com.oc.myflow.executor.service.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.hive.HiveScript;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class HiveServiceImpl implements HiveService {
    private static final Logger appLogger = LoggerFactory.getLogger(HiveServiceImpl.class);
    // record run time in the metric log, metric log is widely used
    private static final Logger metricLogger = LoggerFactory.getLogger(HiveServiceImpl.class);
    @Autowired
    private JdbcTemplate hiveJdbcTemplate; // jdbc, run sql
    @Autowired
    private HiveTemplate hiveTemplate; // run hql
    @Override
    public void execute(String sql) {
        appLogger.info("run execute");
        appLogger.info(sql);
        // execute to present the data
        hiveJdbcTemplate.execute(sql);
        appLogger.info("Hive SQL executed");
    }
    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        appLogger.info("run hive query for list");
        appLogger.info(sql);

        // more widely use case
        // id: 1, name: a, age: 10
        // id: 2, name: b, age: 17
        List<Map<String,Object>> resMap = hiveJdbcTemplate.queryForList(sql);
        // resMap.get(0).get("id)
        appLogger.info("Hive SQL executed");
        return resMap;
    }

    @Override
    public Map<String, Object> queryForMap(String sql) {
        appLogger.info("run hive query for map");
        appLogger.info(sql);
        Map<String,Object> resMap = hiveJdbcTemplate.queryForMap(sql);
        // resMap.get(0).get("id)
        appLogger.info("Hive SQL executed");
        return resMap;
    }
    @Override
    public void runHql(String filePath, Map<String, Object> paramMap){
        appLogger.info("run hive runHql");
        HiveScript hiveScript = new HiveScript(new PathResource(filePath), paramMap);
        hiveTemplate.executeScript(hiveScript);
        appLogger.info("HQL run successfully");
    }
}
