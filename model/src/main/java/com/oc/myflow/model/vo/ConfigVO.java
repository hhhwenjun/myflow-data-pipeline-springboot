package com.oc.myflow.model.vo;

import java.util.List;

/**
 * Create configuration reflection
 */
public class ConfigVO {
    private Integer projectId;
    private String projectName;
    private List<DataSource> dataSource;
    private List<TaskVO> tasks;

    public Integer getProjectId() {
        return projectId;
    }

    public void setProjectId(Integer projectId) {
        this.projectId = projectId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public List<DataSource> getDataSource() {
        return dataSource;
    }

    public void setDataSource(List<DataSource> dataSource) {
        this.dataSource = dataSource;
    }

    public List<TaskVO> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskVO> tasks) {
        this.tasks = tasks;
    }

    /**
     * Create data source inner class
     * <p>Only configVO use it</p>
     */
    public class DataSource {
        private String url;
        private String userName;
        private String password;
        private String driver;
        private String name;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
