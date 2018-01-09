package com.lovecws.mumu.sqoop.basic;

import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

import java.util.Date;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: sqoop创建link
 * @date 2018-01-08 11:18:
 */
public class SqoopLinkOperation {

    private static final Logger log = Logger.getLogger(SqoopLinkOperation.class);

    private SqoopClient sqoopClient;

    public SqoopLinkOperation(SqoopClient sqoopClient) {
        this.sqoopClient = sqoopClient;
    }

    /**
     * 创建mysql连接
     *
     * @param linkName
     * @return
     */
    public MLink createMysqlLink(String linkName) {
        MLink link = sqoopClient.createLink("generic-jdbc-connector");
        link.setName(linkName);
        link.setCreationUser("babymm");
        link.setCreationDate(new Date());

        MLinkConfig linkConfig = link.getConnectorLinkConfig();
        linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://192.168.11.25:3307/mmsns");
        linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
        linkConfig.getStringInput("linkConfig.username").setValue("root");
        linkConfig.getStringInput("linkConfig.password").setValue("123");
        linkConfig.getStringInput("dialect.identifierEnclose").setValue("`");

        Status status = null;
        if (checkExists(linkName)) {
            status = sqoopClient.updateLink(link, linkName);
        } else {
            status = sqoopClient.saveLink(link);
        }

        if (status.canProceed()) {
            log.info("Created Link with Link Name : " + link.getName());
            return link;
        } else {
            log.info("Something went wrong creating the link");
            return null;
        }
    }

    /**
     * 创建hdfs连接
     *
     * @param linkName
     * @return
     */
    public MLink createHdfsLink(String linkName) {
        deleteLinkIfExists(linkName, true);

        MLink link = sqoopClient.createLink("hdfs-connector");
        link.setName(linkName);
        link.setCreationUser("babymm");
        link.setCreationDate(new Date());

        MLinkConfig linkConfig = link.getConnectorLinkConfig();
        linkConfig.getStringInput("linkConfig.uri").setValue("hdfs://192.168.11.25:9000");
        linkConfig.getStringInput("linkConfig.confDir").setValue("/usr/local/hadoop-2.7.3/etc/hadoop");
        Status status = null;
        if (checkExists(linkName)) {
            status = sqoopClient.updateLink(link, linkName);
        } else {
            status = sqoopClient.saveLink(link);
        }
        if (status.canProceed()) {
            log.info("Created Link with Link Name : " + link.getName());
            return link;
        } else {
            log.info("Something went wrong creating the link");
            return null;
        }
    }


    /**
     * 创建ftp连接 ftp连接只能接受数据，不能传递数据。
     *
     * @param linkName
     * @return
     */
    public MLink createFtpLink(String linkName) {
        MLink link = sqoopClient.createLink("ftp-connector");
        link.setName(linkName);
        link.setCreationUser("babymm");
        link.setCreationDate(new Date());

        MLinkConfig linkConfig = link.getConnectorLinkConfig();
        linkConfig.getStringInput("linkConfig.server").setValue("192.168.11.25");
        linkConfig.getIntegerInput("linkConfig.port").setValue(2100);
        linkConfig.getStringInput("linkConfig.username").setValue("root");
        linkConfig.getStringInput("linkConfig.password").setValue("root");
        Status status = null;
        if (checkExists(linkName)) {
            status = sqoopClient.updateLink(link, linkName);
        } else {
            status = sqoopClient.saveLink(link);
        }
        if (status.canProceed()) {
            log.info("Created Link with Link Name : " + link.getName());
            return link;
        } else {
            log.info("Something went wrong creating the link");
            return null;
        }
    }


    /**
     * 创建kafka连接
     *
     * @param linkName
     * @return
     */
    public MLink createKafkaLink(String linkName) {
        MLink link = sqoopClient.createLink("kafka-connector");
        link.setName(linkName);
        link.setCreationUser("babymm");
        link.setCreationDate(new Date());

        MLinkConfig linkConfig = link.getConnectorLinkConfig();
        linkConfig.getStringInput("linkConfig.brokerList").setValue("192.168.11.25:9092");
        linkConfig.getStringInput("linkConfig.zookeeperConnect").setValue("192.168.11.25:2181");
        Status status = null;
        if (checkExists(linkName)) {
            status = sqoopClient.updateLink(link, linkName);
        } else {
            status = sqoopClient.saveLink(link);
        }
        if (status.canProceed()) {
            log.info("Created Link with Link Name : " + link.getName());
            return link;
        } else {
            log.info("Something went wrong creating the link");
            return null;
        }
    }

    /**
     * 删除link
     *
     * @param linkName
     * @return
     */
    public boolean deleteLinkIfExists(String linkName, boolean execusre) {
        boolean checkExists = checkExists(linkName);
        if (checkExists && execusre) {
            sqoopClient.deleteLink(linkName);
        }
        return true;
    }

    /**
     * 删除所有的连接
     */
    public void deleteAllLinks() {
        sqoopClient.deleteAllLinksAndJobs();

        List<MLink> links = sqoopClient.getLinks();
        for (MLink link : links) {
            sqoopClient.deleteLink(link.getName());
        }
    }

    /**
     * 检测link是否存在
     *
     * @param linkName
     * @return
     */
    public boolean checkExists(String linkName) {
        List<MLink> links = sqoopClient.getLinks();
        for (MLink Link : links) {
            if (Link.getName().equals(linkName)) {
                return true;
            }
        }
        return false;
    }
}
