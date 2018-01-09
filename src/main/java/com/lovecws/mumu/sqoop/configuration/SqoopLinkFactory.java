package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.basic.SqoopLinkOperation;
import com.lovecws.mumu.sqoop.configuration.linkconfig.BaseLinkConfig;
import com.lovecws.mumu.sqoop.util.SqoopUtil;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 创建link
 * @date 2018-01-09 13:32:
 */
public class SqoopLinkFactory {

    private static final Logger log = Logger.getLogger(SqoopLinkOperation.class);

    private SqoopClient sqoopClient;

    private SqoopLinkConfig sqoopLinkConfig;

    public SqoopLinkFactory(SqoopClient sqoopClient, SqoopLinkConfig sqoopLinkConfig) {
        this.sqoopClient = sqoopClient;
        this.sqoopLinkConfig = sqoopLinkConfig;
    }

    public MLink instance() {
        BaseLinkConfig baseLinkConfig = sqoopLinkConfig.getLinkConfig();

        MLink link = sqoopClient.createLink(baseLinkConfig.getConnector());
        link.setName(sqoopLinkConfig.getLinkName());
        link.setCreationUser(sqoopLinkConfig.getCreationUser());
        link.setCreationDate(sqoopLinkConfig.getCreationDate());

        MLinkConfig linkConfig = link.getConnectorLinkConfig();

        baseLinkConfig.linkConfig(linkConfig);

        Status status = null;

        if (SqoopUtil.checkExists(sqoopClient, sqoopLinkConfig.getLinkName())) {
            status = sqoopClient.updateLink(link, sqoopLinkConfig.getLinkName());
            log.info("update link with link Name : " + link.getName());
        } else {
            status = sqoopClient.saveLink(link);
        }

        if (status.canProceed()) {
            log.info("Created Link with Link Name : " + link.getName());
            return link;
        } else {
            log.info("Something went wrong creating the link ");
            return null;
        }
    }

    public SqoopClient getSqoopClient() {
        return sqoopClient;
    }

    public void setSqoopClient(SqoopClient sqoopClient) {
        this.sqoopClient = sqoopClient;
    }

    public SqoopLinkConfig getBaseLinkConfig() {
        return sqoopLinkConfig;
    }

    public void setBaseLinkConfig(SqoopLinkConfig sqoopLinkConfig) {
        this.sqoopLinkConfig = sqoopLinkConfig;
    }
}
