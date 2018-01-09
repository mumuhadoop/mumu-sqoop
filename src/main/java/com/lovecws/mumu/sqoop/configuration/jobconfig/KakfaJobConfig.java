package com.lovecws.mumu.sqoop.configuration.jobconfig;

import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MToConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kafka job config
 * @date 2018-01-09 15:51:
 */
public class KakfaJobConfig extends BaseJobConfig {

    private String topic;

    public KakfaJobConfig(String topic) {
        this.topic = topic;
    }

    @Override
    public MFromConfig fromConfig(MFromConfig fromConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MToConfig toConfig(MToConfig toConfig) {
        toConfig.getStringInput("toJobConfig.topic").setValue(topic);
        return toConfig;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
