package com.lovecws.mumu.sqoop.basic;

import com.lovecws.mumu.sqoop.SqoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 开始执行任务
 * @date 2018-01-08 12:02:
 */
public class SqoopBasicOperation {

    SqoopClient sqoopClient = SqoopConfiguration.sqoopClient();
    SqoopJobOperation sqoopJobOperation = new SqoopJobOperation(sqoopClient);
    SqoopLinkOperation sqoopLinkOperation = new SqoopLinkOperation(sqoopClient);

    public void createLinkAndJob() {
        sqoopLinkOperation.deleteAllLinks();

        MLink mysqlLink = sqoopLinkOperation.createMysqlLink("mysql-mmsns-action");
        MLink hdfsLink = sqoopLinkOperation.createHdfsLink("hdfs-mmsns-action");

        sqoopJobOperation.createMysqlToHdfsJob("jdbc2hdfsJob-mmsns-action", mysqlLink.getName(), hdfsLink.getName(), "/mumu/sqoop/mmsns/action");
        sqoopJobOperation.createHdfsToMysqlJob("hdfs2jdbcjob-mmsns-action", "hdfs-mmsns-action", "mysql-mmsns-action", "/mumu/sqoop/mmsns/action");

    }

    public void jdbcTohdfsJob() {
        sqoopJobOperation.startJob("jdbc2hdfsJob-mmsns-action");
        printMessage("/mumu/sqoop/mmsns/action");
    }

    public void hdfsToMysqlJob() {
        sqoopJobOperation.startJob("hdfs2jdbcjob-mmsns-action");
    }

    private void printMessage(String outputDirectory) {
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        try {
            distributedFileSystem.initialize(new URI("hdfs://192.168.11.25:9000"), new Configuration());
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path(outputDirectory));
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    continue;
                }
                FSDataInputStream fsDataInputStream = distributedFileSystem.open(fileStatus.getPath());
                byte[] bs = new byte[fsDataInputStream.available()];
                fsDataInputStream.read(bs);
                System.out.println(new String(bs));
                fsDataInputStream.close();
            }
            distributedFileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

}
