package com.lovecws.mumu.sqoop;

import org.apache.sqoop.client.SqoopClient;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop配置测试
 * @date 2018-01-08 11:49:
 */
public class SqoopConfigurationTest {

    @Test
    public void SqoopClient() {
        SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
        System.out.println(sqoopClient);
    }
}
