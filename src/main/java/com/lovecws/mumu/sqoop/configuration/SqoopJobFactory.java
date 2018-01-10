package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.basic.SqoopLinkOperation;
import com.lovecws.mumu.sqoop.configuration.jobconfig.BaseJobConfig;
import com.lovecws.mumu.sqoop.util.SqoopUtil;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 创建job工厂类
 * @date 2018-01-09 14:56:
 */
public class SqoopJobFactory {
    private static final Logger log = Logger.getLogger(SqoopLinkOperation.class);

    private SqoopClient sqoopClient;

    private SqoopJobConfig sqoopJobConfig;

    private String fromLink;
    private String toLink;

    public SqoopJobFactory(SqoopClient sqoopClient, SqoopJobConfig sqoopJobConfig, String fromLink, String toLink) {
        this.sqoopClient = sqoopClient;
        this.sqoopJobConfig = sqoopJobConfig;
        this.fromLink = fromLink;
        this.toLink = toLink;
    }

    public MJob instance() {
        MJob job = sqoopClient.createJob(fromLink, toLink);
        job.setName(sqoopJobConfig.getJobName());
        job.setCreationUser(sqoopJobConfig.getCreationUser());
        job.setCreationDate(sqoopJobConfig.getCreationDate());

        MFromConfig mFromConfig = job.getFromJobConfig();
        BaseJobConfig fromJobConfig = sqoopJobConfig.getFromJobConfig();
        fromJobConfig.fromConfig(mFromConfig);

        BaseJobConfig toJobConfig = sqoopJobConfig.getToJobConfig();
        MToConfig mToJobConfig = job.getToJobConfig();
        toJobConfig.toConfig(mToJobConfig);

        MDriverConfig driverConfig = job.getDriverConfig();
        driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);
        driverConfig.getIntegerInput("throttlingConfig.numLoaders").setValue(1);

        Status status = null;
        if (SqoopUtil.checkExists(sqoopClient, sqoopJobConfig.getJobName())) {
            status = sqoopClient.updateJob(job, sqoopJobConfig.getJobName());
        } else {
            status = sqoopClient.saveJob(job);
        }
        if (status.canProceed()) {
            log.info("Created Job with Job Name: " + job.getName());
            return job;
        } else {
            log.info("Something went wrong creating the job");
            return null;
        }
    }
}
