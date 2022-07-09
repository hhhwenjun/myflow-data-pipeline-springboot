package com.oc.myflow.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Component
public class CmdUtil {
    private static final Logger appLogger = LoggerFactory.getLogger(CmdUtil.class);

    public int executeCommand(String cmd, String param) throws IOException, InterruptedException {
        Process process = null;
        if (param != null) {
            appLogger.info("Running Command:" + cmd + " " + param);
            // /dev/shm/helloWorld.sh msz
            process = Runtime.getRuntime().exec(cmd + " " + param);
        } else {
            appLogger.info("Running Command:" + cmd);
            process = Runtime.getRuntime().exec(cmd);
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorStream = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String line;
        while ((line = in.readLine()) != null) {
            appLogger.info(line);
        }
        in.close();
        while ((line = errorStream.readLine()) != null) {
            appLogger.info("Error stream " + line);
        }
        errorStream.close();
        process.waitFor();
        int exValue = process.exitValue();
        appLogger.info(cmd + " exit value: " + exValue);
        process.destroy();
        return exValue;
    }

    public int executeCommand(String[] cmd) throws IOException, InterruptedException {
        Process process = null;
        appLogger.info("Running Command:" + cmd);
        process = Runtime.getRuntime().exec(cmd);

        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorStream = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String line;
        while ((line = in.readLine()) != null) {
            appLogger.info(line);
        }
        in.close();
        while ((line = errorStream.readLine()) != null) {
            appLogger.info("Error stream " + line);
        }
        errorStream.close();
        process.waitFor();
        int exValue = process.exitValue();
        appLogger.info(cmd + " exit value: " + exValue);
        process.destroy();
        return exValue;
    }
}
