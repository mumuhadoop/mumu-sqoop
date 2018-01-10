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

    private String sql;
    private String partitionColumn;
    private boolean allowNullValueInPartitionColumn;
    private String boundaryQuery;

    public JdbcJobConfig(String schemaName, String sql) {
        this.schemaName = schemaName;
        this.sql = sql;
    }

    public JdbcJobConfig(String schemaName, String tableName, String columnList) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnList = columnList;
    }

    public JdbcJobConfig(String schemaName, String sql, String partitionColumn, boolean allowNullValueInPartitionColumn, String boundaryQuery) {
        this.schemaName = schemaName;
        this.sql = sql;
        this.partitionColumn = partitionColumn;
        this.allowNullValueInPartitionColumn = allowNullValueInPartitionColumn;
        this.boundaryQuery = boundaryQuery;
    }

    @Override
    public MFromConfig fromConfig(MFromConfig fromConfig) {
        fromConfig.getStringInput("fromJobConfig.schemaName").setValue(schemaName);
        if (tableName != null) {
            fromConfig.getStringInput("fromJobConfig.tableName").setValue(tableName);
        }
        if (columnList != null) {
            fromConfig.getListInput("fromJobConfig.columnList").setValue(Arrays.asList(columnList.split(",")));
        }
        if (sql != null) {
            fromConfig.getStringInput("fromJobConfig.sql").setValue(sql);
        }
        if (partitionColumn != null) {
            fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue(partitionColumn);
            fromConfig.getBooleanInput("fromJobConfig.allowNullValueInPartitionColumn").setValue(allowNullValueInPartitionColumn);
        }
        if (boundaryQuery != null) {
            fromConfig.getStringInput("fromJobConfig.boundaryQuery").setValue(boundaryQuery);
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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public boolean isAllowNullValueInPartitionColumn() {
        return allowNullValueInPartitionColumn;
    }

    public void setAllowNullValueInPartitionColumn(boolean allowNullValueInPartitionColumn) {
        this.allowNullValueInPartitionColumn = allowNullValueInPartitionColumn;
    }

    public String getBoundaryQuery() {
        return boundaryQuery;
    }

    public void setBoundaryQuery(String boundaryQuery) {
        this.boundaryQuery = boundaryQuery;
    }
}
