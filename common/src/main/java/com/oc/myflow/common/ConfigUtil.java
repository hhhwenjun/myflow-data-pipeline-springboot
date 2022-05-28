package com.oc.myflow.common;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.oc.myflow.model.vo.ConfigVO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.FileReader;

@Component
public class ConfigUtil {
    @Value("${jsonConfigPath}")
    private String jsonConfigPath;
    public ConfigVO getConfigVO() throws FileNotFoundException {
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(jsonConfigPath));
        // deserialize JSON and convert to an object
        ConfigVO configVO = gson.fromJson("test.json", ConfigVO.class);
        return configVO;
    }
}
