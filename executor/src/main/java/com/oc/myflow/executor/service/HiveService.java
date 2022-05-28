package com.oc.myflow.executor.service;

import java.util.List;
import java.util.Map;

/**
 * Define functions of Hive that we want to execute
 */
public interface HiveService {

    void execute(String sql);

    List<Map<String, Object>> queryForList(String sql);

    Map<String, Object> queryForMap(String sql);

    void runHql(String filePath, Map<String, Object> paramMap);
}
