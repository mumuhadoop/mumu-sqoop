package com.lovecws.mumu.sqoop;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;

import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop的配置信息
 * @date 2018-01-08 11:13:
 */
public class SqoopConfiguration {

    // 注意sqoop 后面有个 /,如果没有会报下面的错,非常诡异
    // Exception: org.apache.sqoop.common.SqoopException Message: CLIENT_0004:Unable to find valid Kerberos ticket cache (kinit)
    public static final String SQOOP_SERVER_ADDRESS = "http://192.168.11.25:12000/sqoop/";

    public static SqoopClient sqoopClient() {
        return new SqoopClient(SQOOP_SERVER_ADDRESS);
    }
}
