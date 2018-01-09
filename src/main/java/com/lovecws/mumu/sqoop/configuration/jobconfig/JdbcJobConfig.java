package com.lovecws.mumu.sqoop.configuration.jobconfig;

import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MToConfig;

import java.util.Arrays;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: jdbc from link
 * @date 2018-01-09 14:21:
 */
public class JdbcJobConfig extends BaseJobConfig {

    private String schemaName;
    private String tableName;
    private String columnList;

    public JdbcJobConfig(String schemaName, String tableName, String columnList) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnList = columnList;
    }

    @Override
    public MFromConfig fromConfig(MFromConfig fromConfig) {
        fromConfig.getStringInput("fromJobConfig.schemaName").setValue(schemaName);
        fromConfig.getStringInput("fromJobConfig.tableName").setValue(tableName);
        if (columnList != null) {
            fromConfig.getListInput("fromJobConfig.columnList").setValue(Arrays.asList(columnList.split(",")));
        }
        return fromConfig;
    }

    @Override
    public MToConfig toConfig(MToConfig toConfig) {
        toConfig.getStringInput("toJobConfig.schemaName").setValue(schemaName);
        toConfig.getStringInput("toJobConfig.tableName").setValue(tableName);
        if (columnList != null) {
            toConfig.getListInput("toJobConfig.columnList").setValue(Arrays.asList(columnList.split(",")));
        }
        return toConfig;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnList() {
        return columnList;
    }

    public void setColumnList(String columnList) {
        this.columnList = columnList;
    }
}
