package com.lovecws.mumu.sqoop.configuration.linkconfig;

import org.apache.sqoop.model.MLinkConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 基本jdbc连接
 * @date 2018-01-09 13:19:
 */
public class JdbcLinkConfig extends BaseLinkConfig {

    private static final String connectorName = "generic-jdbc-connector";

    private String connectionString;

    private String jdbcDriver;

    private String username;

    private String password;

    private String identifierEnclose;

    public JdbcLinkConfig(String connectionString, String jdbcDriver, String username, String password, String identifierEnclose) {
        this.connectionString = connectionString;
        this.jdbcDriver = jdbcDriver;
        this.username = username;
        this.password = password;
        this.identifierEnclose = identifierEnclose;
    }

    @Override
    public MLinkConfig linkConfig(MLinkConfig linkConfig) {
        linkConfig.getStringInput("linkConfig.connectionString").setValue(connectionString);
        linkConfig.getStringInput("linkConfig.jdbcDriver").setValue(jdbcDriver);
        linkConfig.getStringInput("linkConfig.username").setValue(username);
        linkConfig.getStringInput("linkConfig.password").setValue(password);
        linkConfig.getStringInput("dialect.identifierEnclose").setValue(identifierEnclose);
        return linkConfig;
    }

    @Override
    public String getConnector() {
        return connectorName;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getIdentifierEnclose() {
        return identifierEnclose;
    }

    public void setIdentifierEnclose(String identifierEnclose) {
        this.identifierEnclose = identifierEnclose;
    }
}
