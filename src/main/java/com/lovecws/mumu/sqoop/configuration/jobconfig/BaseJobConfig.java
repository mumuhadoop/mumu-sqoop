package com.lovecws.mumu.sqoop.configuration.jobconfig;

import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MToConfig;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 基本link配置信息
 * @date 2018-01-09 13:22:
 */
public abstract class BaseJobConfig {

    public abstract MFromConfig fromConfig(MFromConfig fromConfig);

    public abstract MToConfig toConfig(MToConfig toConfig);
}
