package com.lovecws.mumu.sqoop.basic;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 数据转换操作
 * @date 2018-01-08 12:07:
 */
public class SqoopBasicOperationTest {

    private SqoopBasicOperation sqoopBasicOperation = new SqoopBasicOperation();

    @Test
    public void createLinkAndJob() throws URISyntaxException, IOException {
        sqoopBasicOperation.createLinkAndJob();
    }

    @Test
    public void jdbcTohdfsJob() throws URISyntaxException, IOException {
        sqoopBasicOperation.jdbcTohdfsJob();
    }

    @Test
    public void hdfsToMysqlJob() {
        sqoopBasicOperation.hdfsToMysqlJob();
    }
}
