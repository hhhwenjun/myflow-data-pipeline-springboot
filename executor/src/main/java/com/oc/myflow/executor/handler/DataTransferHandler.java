package com.oc.myflow.executor.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class DataTransferHandler implements RowCallbackHandler {

    private static final Logger appLogger = LoggerFactory.getLogger(DataTransferHandler.class);

    // reduce database access times
    private int cache = 3000;
    private List<String> keyList;
    private JdbcTemplate destJtm;
    private String destTable;
    private List<Map<String, Object>> rowList = new ArrayList<>();

    public DataTransferHandler(JdbcTemplate destJtm, String destTable){
        this.destJtm = destJtm;
        this.destTable = destTable;
    }

    // resultSet means each line of the data
    @Override
    public void processRow(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= columnCount; i++){
            String[] strArr = metaData.getColumnName(i).split("\\.");
            row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        rowList.add(row);
        if (rowList.size() == cache){
            batchInsert(rowList);
            rowList.clear();
        }
    }

    // in case we have a small amount of data left in list
    public void saveRest(){
        if (!rowList.isEmpty()){
            appLogger.info("Begin to add the rest " + rowList.size() + " records");
            batchInsert(rowList);
            appLogger.info("The rest " + rowList.size() + " records are added");
        }
    }

    private void batchInsert(List<Map<String, Object>> res){
        appLogger.info("Begin batch insert " + res.size() + " records to" + destTable);
        Set<String> keySet = res.get(0).keySet();
        List<String> keyList = new ArrayList<>(keySet);

        List<String> valueStrList = new ArrayList<>();
        res.forEach(rowMap -> {
            List<String> valueList = new ArrayList<>();
            keyList.forEach(key -> {
                valueList.add("'" +
                        String.valueOf(rowMap.get(key)) + "'");
            });
            String valueStr = "(" + String.join(",", valueList) + ")";
            valueStrList.add(valueStr);
        });
        appLogger.info("insert into " + destTable +
                " " + String.join(",", valueStrList));
        destJtm.execute("insert into " + destTable
                + "(" + String.join(",", keyList)
                + ") values " + String.join(",", valueStrList));
        appLogger.info("Begin batch insertion is done");
    }
}
