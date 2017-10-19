package com.lovecws.mumu.sqoop;

import org.apache.sqoop.client.SqoopClient;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-19 15:57
 */
public class SqoopClientOperation {
    public static void main(String[] args) {
        SqoopClient sqoopClient = new SqoopClient("");
        sqoopClient.createLink("name");
        System.out.println(sqoopClient);
    }
}
