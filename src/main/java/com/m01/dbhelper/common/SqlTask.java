package com.m01.dbhelper.common;

import java.util.List;

public class SqlTask {
    private String taskName;
    //当执行sql失败时的处理策略
    private String policyWhenError;
    ;
    private List<String> sqlList;

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getPolicyWhenError() {
        return policyWhenError;
    }

    public void setPolicyWhenError(String policyWhenError) {
        this.policyWhenError = policyWhenError;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }


}
