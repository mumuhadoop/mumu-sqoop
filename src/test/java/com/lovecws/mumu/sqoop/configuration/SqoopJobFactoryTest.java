package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import com.lovecws.mumu.sqoop.configuration.jobconfig.HdfsJobConfig;
import com.lovecws.mumu.sqoop.configuration.jobconfig.JdbcJobConfig;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: job工厂类测试
 * @date 2018-01-09 15:04:
 */
public class SqoopJobFactoryTest {

    @Test
    public void jdbcToHdfs() {
        SqoopJobConfig sqoopJobConfig = new SqoopJobConfig("job-factory-hive", "babymm",
                new JdbcJobConfig("mmsns", "ma_action", null),
                new HdfsJobConfig("/mumu/sqoop/mmsns/action", true, "N", "SEQUENCE_FILE", "NONE", true));

        SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
        SqoopJobFactory sqoopJobFactory = new SqoopJobFactory(sqoopClient,
                sqoopJobConfig,
                "mysql-mmsns-action",
                "hdfs-mmsns-action");
        MJob job = sqoopJobFactory.instance();
        System.out.println(job);
    }

    @Test
    public void hdfsToJdbc() {
        SqoopJobConfig sqoopJobConfig = new SqoopJobConfig("hdfsToJdbcjob-factory-hive", "babymm",
                new HdfsJobConfig("/mumu/sqoop/mmsns/action", true, "N"),
                new JdbcJobConfig("mmsns", "ma_action", null));

        SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
        SqoopJobFactory sqoopJobFactory = new SqoopJobFactory(sqoopClient,
                sqoopJobConfig,
                "hdfs-mmsns-action",
                "mysql-mmsns-action");
        MJob job = sqoopJobFactory.instance();
        System.out.println(job);
    }
}
