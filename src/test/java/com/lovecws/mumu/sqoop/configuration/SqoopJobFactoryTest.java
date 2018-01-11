package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import com.lovecws.mumu.sqoop.configuration.jobconfig.HdfsJobConfig;
import com.lovecws.mumu.sqoop.configuration.jobconfig.JdbcJobConfig;
import com.lovecws.mumu.sqoop.configuration.jobconfig.KiteJobConfig;
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

    /**
     * 将jdbc的数据导出到kite数据集中
     */
    @Test
    public void hdfsToKite() {
        SqoopJobConfig sqoopJobConfig = new SqoopJobConfig("hdfsToKitejob", "babymm",
                new JdbcJobConfig("mmsns", "ma_action", null),
                new KiteJobConfig("dataset:hdfs://192.168.11.25:9000/mumu/sqoop/kitenamespace/kitedataset", "AVRO"));

        SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
        SqoopJobFactory sqoopJobFactory = new SqoopJobFactory(sqoopClient,
                sqoopJobConfig,
                "mysql-mmsns-action",
                "kite-link");
        MJob job = sqoopJobFactory.instance();
        System.out.println(job);
    }


    /**
     * hdfs中的kite数据集导出到本地数据集 error
     */
    @Test
    public void kiteToKitejob() {
        SqoopJobConfig sqoopJobConfig = new SqoopJobConfig("kiteToKiteJob", "babymm",
                new KiteJobConfig("dataset:hdfs://192.168.11.25:9000/mumu/sqoop/kitenamespace/kitedataset"),
                new KiteJobConfig("dataset:file:///mumu/sqoop/kitenamespace/kitedataset", "AVRO"));

        SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
        SqoopJobFactory sqoopJobFactory = new SqoopJobFactory(sqoopClient,
                sqoopJobConfig,
                "kite-link",
                "kite-link2");
        MJob job = sqoopJobFactory.instance();
        System.out.println(job);
    }

    /**
     * hdfs中的kite数据集导出到本地数据集 error
     */
    @Test
    public void kiteToHdfsJob() {
        SqoopJobConfig sqoopJobConfig = new SqoopJobConfig("kiteToHdfsJob", "babymm",
                new KiteJobConfig("dataset:hdfs://192.168.11.25:9000/mumu/sqoop/kitenamespace/kitedataset"),
                new HdfsJobConfig("/mumu/sqoop/mmsns/kite", true, "N"));

        SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
        SqoopJobFactory sqoopJobFactory = new SqoopJobFactory(sqoopClient,
                sqoopJobConfig,
                "kite-link",
                "hdfs-mmsns-action");
        MJob job = sqoopJobFactory.instance();
        System.out.println(job);
    }
}
