package com.lovecws.mumu.sqoop.configuration.jobconfig;

import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MToConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kafka job config
 * @date 2018-01-09 15:51:
 */
public class KiteJobConfig extends BaseJobConfig {

    private String uri;
    private String fileFormat;//CSV,AVRO,PARQUET

    public KiteJobConfig(String uri) {
        this.uri = uri;
    }

    public KiteJobConfig(String uri, String fileFormat) {
        this.uri = uri;
        this.fileFormat = fileFormat;
    }

    @Override
    public MFromConfig fromConfig(MFromConfig fromConfig) {
        fromConfig.getStringInput("fromJobConfig.uri").setValue(uri);
        return fromConfig;
    }

    @Override
    public MToConfig toConfig(MToConfig toConfig) {
        toConfig.getStringInput("toJobConfig.uri").setValue(uri);
        toConfig.getEnumInput("toJobConfig.fileFormat").setValue(fileFormat);
        return toConfig;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
    }
}
