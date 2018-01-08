package com.lovecws.mumu.sqoop.basic;

import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop创建job
 * @date 2018-01-08 11:26:
 */
public class SqoopJobOperation {

    private static final Logger log = Logger.getLogger(SqoopJobOperation.class);

    private SqoopClient sqoopClient;

    public SqoopJobOperation(SqoopClient sqoopClient) {
        this.sqoopClient = sqoopClient;
    }

    /**
     * 创建从mysql数据库中导出数据到hdfs中
     *
     * @param jobName
     * @param fromLink
     * @param toLink
     * @param outputDirectory
     * @return
     */
    public MJob createMysqlToHdfsJob(String jobName, String fromLink, String toLink, String outputDirectory) {
        MJob job = sqoopClient.createJob(fromLink, toLink);
        job.setName(jobName);
        job.setCreationUser("babymumu");
        job.setCreationDate(new Date());

        MFromConfig fromJobConfig = job.getFromJobConfig();

        fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("mmsns");
        fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("ma_action");
        fromJobConfig.getListInput("fromJobConfig.columnList").setValue(Arrays.asList("action_id", "action_status", "action_date", "action_type", "action_content",
                "word_count", "user_id", "collect_count", "vote_count", "comment_count",
                "reprint_count", "reprint_action_content", "reprint_action_id", "reprint_user_id"));

        MToConfig toJobConfig = job.getToJobConfig();
        toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(outputDirectory);
        toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE");
        toJobConfig.getEnumInput("toJobConfig.compression").setValue("NONE");
        toJobConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(true);

        MDriverConfig driverConfig = job.getDriverConfig();
        driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

        Status status = null;
        if (checkExistsJob(jobName)) {
            status = sqoopClient.updateJob(job, jobName);
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

    /**
     * 创建从hdfs中导出数据到mysql中
     *
     * @param jobName
     * @param fromLink
     * @param toLink
     * @param inputDirectory
     * @return
     */
    public MJob createHdfsToMysqlJob(String jobName, String fromLink, String toLink, String inputDirectory) {
        MJob job = sqoopClient.createJob(fromLink, toLink);
        job.setName(jobName);
        job.setCreationUser("babymumu");
        job.setCreationDate(new Date());

        MFromConfig fromJobConfig = job.getFromJobConfig();

        fromJobConfig.getStringInput("fromJobConfig.inputDirectory").setValue(inputDirectory);

        MToConfig toJobConfig = job.getToJobConfig();
        toJobConfig.getStringInput("toJobConfig.schemaName").setValue("mmsns");
        toJobConfig.getStringInput("toJobConfig.tableName").setValue("ma_sqoop_action");
        toJobConfig.getListInput("toJobConfig.columnList").setValue(Arrays.asList("action_id", "action_status", "action_date", "action_type", "action_content",
                "word_count", "user_id", "collect_count", "vote_count", "comment_count",
                "reprint_count", "reprint_action_content", "reprint_action_id", "reprint_user_id"));

        MDriverConfig driverConfig = job.getDriverConfig();
        driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

        Status status = null;
        if (checkExistsJob(jobName)) {
            status = sqoopClient.updateJob(job, jobName);
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

    /**
     * 开始执行任务
     *
     * @param jobName
     */
    public void startJob(String jobName) {
        MSubmission submission = sqoopClient.startJob(jobName);
        log.info("Job Submission Status : " + submission.getStatus());
        while (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            log.info("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
        }

        log.info("Hadoop job id :" + submission.getExternalJobId());
        log.info("Job link : " + submission.getExternalLink());
        Counters counters = submission.getCounters();
        if (counters != null) {
            log.info("Counters:");
            for (CounterGroup group : counters) {
                System.out.print("\t");
                log.info(group.getName());
                for (Counter counter : group) {
                    System.out.print("\t\t");
                    System.out.print(counter.getName());
                    System.out.print(": ");
                    log.info(counter.getValue());
                }
            }
        }
    }

    /**
     * 检测工作是否存在
     *
     * @param jobName
     * @return
     */
    public boolean checkExistsJob(String jobName) {
        List<MJob> jobs = sqoopClient.getJobs();
        for (MJob job : jobs) {
            if (job.getName().equals(jobName)) {
                return true;
            }
        }
        return false;
    }
}
