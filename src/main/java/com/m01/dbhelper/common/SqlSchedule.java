package com.m01.dbhelper.common;

import java.util.List;

/**
 *
 */
public class SqlSchedule {
    private String scheduleName;
    private String policyWhenError;;
    private DbType dbType;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private String resultFilePath; // 输出结果保存文件的全路径
    private List<SqlTask> taskList;

    public String getScheduleName() {
        return scheduleName;
    }

    public void setScheduleName(String scheduleName) {
        this.scheduleName = scheduleName;
    }

    public String getPolicyWhenError() {
        return policyWhenError;
    }

    public void setPolicyWhenError(String policyWhenError) {
        this.policyWhenError = policyWhenError;
    }

    public DbType getDbType() {
        return dbType;
    }

    public void setDbType(DbType dbType) {
        this.dbType = dbType;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getResultFilePath() {
        return resultFilePath;
    }

    public void setResultFilePath(String resultFilePath) {
        this.resultFilePath = resultFilePath;
    }

    public List<SqlTask> getTaskList() {
        return taskList;
    }

    public void setTaskList(List<SqlTask> taskList) {
        this.taskList = taskList;
    }
}
