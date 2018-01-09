package com.lovecws.mumu.sqoop.configuration.linkconfig;

import org.apache.sqoop.model.MLinkConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kite link config
 * @date 2018-01-09 17:21:
 */
public class KiteLinkConfig extends BaseLinkConfig{

    private String authority;

    public KiteLinkConfig(String authority) {
        this.authority = authority;
    }

    @Override
    public MLinkConfig linkConfig(MLinkConfig linkConfig) {
        linkConfig.getStringInput("linkConfig.authority").setValue(authority);
        return linkConfig;
    }

    @Override
    public String getConnector() {
        return "kite-connector";
    }

    public String getAuthority() {
        return authority;
    }

    public void setAuthority(String authority) {
        this.authority = authority;
    }
}
