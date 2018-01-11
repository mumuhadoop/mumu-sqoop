package com.lovecws.mumu.sqoop.util;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop测试
 * @date 2018-01-11 15:20:
 */
public class SqoopUtilTest {

    @Test
    public void startHdfsToKitejobJob() {
        SqoopUtil.startJob(SqoopConfiguration.sqoopClient(), "hdfsToKitejob");
    }

    @Test
    public void startKiteToKiteJob() {
        SqoopUtil.startJob(SqoopConfiguration.sqoopClient(), "kiteToKiteJob");
    }
}
