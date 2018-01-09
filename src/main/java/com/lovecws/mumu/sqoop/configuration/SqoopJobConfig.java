package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.configuration.jobconfig.BaseJobConfig;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop jobconfig
 * @date 2018-01-09 14:54:
 */
public class SqoopJobConfig {

    private String jobName;

    private String creationUser;

    private Date creationDate;

    private BaseJobConfig fromJobConfig;

    private BaseJobConfig toJobConfig;

    public SqoopJobConfig(String jobName, String creationUser, BaseJobConfig fromJobConfig, BaseJobConfig toJobConfig) {
        this.jobName = jobName;
        this.creationUser = creationUser;
        this.creationDate = new Date();
        this.fromJobConfig = fromJobConfig;
        this.toJobConfig = toJobConfig;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getCreationUser() {
        return creationUser;
    }

    public void setCreationUser(String creationUser) {
        this.creationUser = creationUser;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public BaseJobConfig getFromJobConfig() {
        return fromJobConfig;
    }

    public void setFromJobConfig(BaseJobConfig fromJobConfig) {
        this.fromJobConfig = fromJobConfig;
    }

    public BaseJobConfig getToJobConfig() {
        return toJobConfig;
    }

    public void setToJobConfig(BaseJobConfig toJobConfig) {
        this.toJobConfig = toJobConfig;
    }
}
