package com.lovecws.mumu.sqoop.configuration.linkconfig;

import org.apache.sqoop.model.MLinkConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 基本jdbc连接
 * @date 2018-01-09 13:19:
 */
public class KafkaLinkConfig extends BaseLinkConfig {

    private static final String connectorName = "kafka-connector";

    private String brokerList;

    private String zookeeperConnect;

    public KafkaLinkConfig(String brokerList, String zookeeperConnect) {
        this.brokerList = brokerList;
        this.zookeeperConnect = zookeeperConnect;
    }

    @Override
    public MLinkConfig linkConfig(MLinkConfig linkConfig) {
        linkConfig.getStringInput("linkConfig.brokerList").setValue(brokerList);
        linkConfig.getStringInput("linkConfig.zookeeperConnect").setValue(zookeeperConnect);
        return linkConfig;
    }

    @Override
    public String getConnector() {
        return connectorName;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }
}
