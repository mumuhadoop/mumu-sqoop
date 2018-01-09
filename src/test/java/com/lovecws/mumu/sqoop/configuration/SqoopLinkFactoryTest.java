package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import com.lovecws.mumu.sqoop.configuration.linkconfig.JdbcLinkConfig;
import com.lovecws.mumu.sqoop.configuration.linkconfig.KiteLinkConfig;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 测试工厂类
 * @date 2018-01-09 14:13:
 */
public class SqoopLinkFactoryTest {

    @Test
    public void instance() {
        SqoopLinkConfig sqoopLinkConfig = new SqoopLinkConfig("base-link",
                "babymm",
                new JdbcLinkConfig("jdbc:mysql://192.168.11.25:3307/mmsns", "com.mysql.jdbc.Driver", "root", "123", "`"));
        SqoopLinkFactory factory = new SqoopLinkFactory(SqoopConfiguration.sqoopClient(),
                sqoopLinkConfig);
        MLink mLink = factory.instance();
        System.out.println(mLink);
    }

    @Test
    public void createKiteLink() {
        SqoopLinkConfig sqoopLinkConfig = new SqoopLinkConfig("kite-link",
                "babymm",
                new KiteLinkConfig("hdfs://192.168.11.25:9000/mumu"));
        SqoopLinkFactory factory = new SqoopLinkFactory(SqoopConfiguration.sqoopClient(),
                sqoopLinkConfig);
        MLink mLink = factory.instance();
        System.out.println(mLink);
    }
}
