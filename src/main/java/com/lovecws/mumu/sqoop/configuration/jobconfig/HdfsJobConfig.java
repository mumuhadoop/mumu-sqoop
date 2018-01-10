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

    private static final String DEFAULT_OUTPUT_FORMAT = "TEXT_FILE";
    private static final String DEFAULT_OUTPUT_COMPRESSION = "NONE";
    private String inputDirectory;
    private String nullValue;
    private boolean overrideNullValue;

    private String outputDirectory;
    private String outputFormat;//TEXT_FILE,SEQUENCE_FILE,PARQUET_FILE
    private String compression;//NONE,DEFAULT,DEFLATE,GZIP,BZIP2,LZO,LZ4,SNAPPY,CUSTOM
    private boolean appendMode;
    private String customCompression;

    public HdfsJobConfig(String inputDirectory, boolean overrideNullValue, String nullValue) {
        this.inputDirectory = inputDirectory;
        this.nullValue = nullValue;
        this.overrideNullValue = overrideNullValue;
    }

    public HdfsJobConfig(String outputDirectory, boolean overrideNullValue, String nullValue, String outputFormat, String compression, boolean appendMode) {
        this.nullValue = nullValue;
        this.overrideNullValue = overrideNullValue;
        this.outputDirectory = outputDirectory;
        this.outputFormat = outputFormat != null ? outputFormat : DEFAULT_OUTPUT_FORMAT;
        this.compression = compression != null ? compression : DEFAULT_OUTPUT_COMPRESSION;
        this.appendMode = appendMode;
    }

    @Override
    public MFromConfig fromConfig(MFromConfig fromConfig) {
        fromConfig.getStringInput("fromJobConfig.inputDirectory").setValue(inputDirectory);
        if (overrideNullValue) {
            fromConfig.getStringInput("fromJobConfig.nullValue").setValue(nullValue);
            fromConfig.getBooleanInput("fromJobConfig.overrideNullValue").setValue(overrideNullValue);
        }
        return fromConfig;
    }

    @Override
    public MToConfig toConfig(MToConfig toConfig) {
        toConfig.getStringInput("toJobConfig.outputDirectory").setValue(outputDirectory);
        toConfig.getEnumInput("toJobConfig.outputFormat").setValue(outputFormat);
        toConfig.getEnumInput("toJobConfig.compression").setValue(compression);
        toConfig.getBooleanInput("toJobConfig.appendMode").setValue(appendMode);
        if (overrideNullValue) {
            toConfig.getStringInput("toJobConfig.nullValue").setValue(nullValue);
            toConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(overrideNullValue);
        }
        if (compression != null && "CUSTOM".equalsIgnoreCase(compression)) {
            toConfig.getEnumInput("toJobConfig.customCompression").setValue(customCompression);
        }
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

    public boolean isAppendMode() {
        return appendMode;
    }

    public void setAppendMode(boolean appendMode) {
        this.appendMode = appendMode;
    }

    public String getCustomCompression() {
        return customCompression;
    }

    public void setCustomCompression(String customCompression) {
        this.customCompression = customCompression;
    }
}
