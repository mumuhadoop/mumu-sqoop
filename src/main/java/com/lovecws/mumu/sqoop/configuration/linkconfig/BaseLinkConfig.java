package com.lovecws.mumu.sqoop.configuration.linkconfig;

import org.apache.sqoop.model.MLinkConfig;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 基本link配置信息
 * @date 2018-01-09 13:22:
 */
public abstract class BaseLinkConfig {

    public abstract MLinkConfig linkConfig(MLinkConfig mLinkConfig);

    public abstract String getConnector();
}
