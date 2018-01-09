package com.lovecws.mumu.sqoop.basic;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop连接测试
 * @date 2018-01-08 11:48:
 */
public class SqoopLinkOperationTest {

    private SqoopLinkOperation sqoopLinkOperation = new SqoopLinkOperation(SqoopConfiguration.sqoopClient());

    @Test
    public void deleteLink() {
        sqoopLinkOperation.deleteLinkIfExists("mysql-mmsns-action", true);
    }

    @Test
    public void deleteAllLinks() {
        sqoopLinkOperation.deleteAllLinks();
    }

    @Test
    public void createFtpLink() {
        sqoopLinkOperation.createFtpLink("ftplink");
    }

    @Test
    public void createKafkaLink() {
        sqoopLinkOperation.createKafkaLink("kafkaLink");
    }
}
