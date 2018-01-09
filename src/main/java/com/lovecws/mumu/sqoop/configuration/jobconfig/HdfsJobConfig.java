package com.lovecws.mumu.sqoop.configuration.jobconfig;

import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MToConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hdfs jobconfig
 * @date 2018-01-09 14:25:
 */
public class HdfsJobConfig extends BaseJobConfig {

    private String inputDirectory;
    private String nullValue;
    private boolean overrideNullValue;

    private String outputDirectory;
    private String outputFormat;
    private String compression;
    private boolean appendMode;

    public HdfsJobConfig(String inputDirectory, String nullValue, boolean overrideNullValue) {
        this.inputDirectory = inputDirectory;
        this.nullValue = nullValue;
        this.overrideNullValue = overrideNullValue;
    }

    public HdfsJobConfig(String outputDirectory, String nullValue, boolean overrideNullValue, String outputFormat, String compression, boolean appendMode) {
        this.nullValue = nullValue;
        this.overrideNullValue = overrideNullValue;
        this.outputDirectory = outputDirectory;
        this.outputFormat = outputFormat;
        this.compression = compression;
        this.appendMode = appendMode;
    }

    @Override
    public MFromConfig fromConfig(MFromConfig fromConfig) {
        fromConfig.getStringInput("fromJobConfig.inputDirectory").setValue(inputDirectory);
        fromConfig.getStringInput("fromJobConfig.nullValue").setValue(nullValue);
        fromConfig.getBooleanInput("fromJobConfig.overrideNullValue").setValue(overrideNullValue);
        return fromConfig;
    }

    @Override
    public MToConfig toConfig(MToConfig toConfig) {
        toConfig.getStringInput("toJobConfig.outputDirectory").setValue(outputDirectory);
        toConfig.getEnumInput("toJobConfig.outputFormat").setValue(outputFormat);
        toConfig.getEnumInput("toJobConfig.compression").setValue(compression);
        toConfig.getBooleanInput("toJobConfig.appendMode").setValue(appendMode);
        toConfig.getStringInput("toJobConfig.nullValue").setValue(nullValue);
        toConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(overrideNullValue);
        return toConfig;
    }

    public String getInputDirectory() {
        return inputDirectory;
    }

    public void setInputDirectory(String inputDirectory) {
        this.inputDirectory = inputDirectory;
    }

    public String getNullValue() {
        return nullValue;
    }

    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    public boolean isOverrideNullValue() {
        return overrideNullValue;
    }

    public void setOverrideNullValue(boolean overrideNullValue) {
        this.overrideNullValue = overrideNullValue;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public boolean getAppendMode() {
        return appendMode;
    }

    public void setAppendMode(boolean appendMode) {
        this.appendMode = appendMode;
    }
}
