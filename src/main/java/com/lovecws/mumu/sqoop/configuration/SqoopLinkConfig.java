package com.lovecws.mumu.sqoop.configuration;

import com.lovecws.mumu.sqoop.configuration.linkconfig.BaseLinkConfig;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 基本link配置信息
 * @date 2018-01-09 13:22:
 */
public class SqoopLinkConfig {

    private String linkName;

    private String creationUser;

    private Date creationDate;

    private BaseLinkConfig linkConfig;

    public SqoopLinkConfig(String linkName, String creationUser, BaseLinkConfig linkConfig) {
        this.linkName = linkName;
        this.creationUser = creationUser;
        this.creationDate = new Date();
        this.linkConfig = linkConfig;
    }

    public String getLinkName() {
        return linkName;
    }

    public void setLinkName(String linkName) {
        this.linkName = linkName;
    }

    public String getCreationUser() {
        return creationUser;
    }

    public void setCreationUser(String creationUser) {
        this.creationUser = creationUser;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public BaseLinkConfig getLinkConfig() {
        return linkConfig;
    }

    public void setLinkConfig(BaseLinkConfig linkConfig) {
        this.linkConfig = linkConfig;
    }
}
