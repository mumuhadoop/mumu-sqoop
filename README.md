## mumu-sqoop 大数据导入和导出

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mumuhadoop/mumu-sqoop/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/mumuhadoop/mumu-sqoop.svg?branch=master)](https://travis-ci.org/mumuhadoop/mumu-sqoop)
[![codecov](https://codecov.io/gh/mumuhadoop/mumu-sqoop/branch/master/graph/badge.svg)](https://codecov.io/gh/mumuhadoop/mumu-sqoop)

mumu-sqoop是一个demo程序，主要通过这个项目来了解sqoop的使用方式和运行原理,为mmsns项目的大数据导出做准备。使用sqoop可以将数据库里面的数据导出到hdfs中，然后通过大数据分析工具(pig、hive、spark、mahout等)来分析导出的数据，实现数据的ETL。并且导出数据可以指定导出文件格式(TEXT_FILE、SEQUENCE_FILE、PARQUET_FILE)和是否q启用压缩(NONE,DEFAULT,DEFLATE,GZIP,BZIP2,LZO,LZ4,SNAPPY,CUSTOM)。


## sqoop
 
### sqoop支持的connector  
Name | Version | Class | Supported Directions
----|----|----|----
generic-jdbc-connector | 1.99.7 | org.apache.sqoop.connector.jdbc.GenericJdbcConnector        | FROM/TO
kite-connector         | 1.99.7 | org.apache.sqoop.connector.kite.KiteConnector               | FROM/TO
oracle-jdbc-connector  | 1.99.7 | org.apache.sqoop.connector.jdbc.oracle.OracleJdbcConnector  | FROM/TO
ftp-connector          | 1.99.7 | org.apache.sqoop.connector.ftp.FtpConnector                 | TO
hdfs-connector         | 1.99.7 | org.apache.sqoop.connector.hdfs.HdfsConnector               | FROM/TO
kafka-connector        | 1.99.7 | org.apache.sqoop.connector.kafka.KafkaConnector             | TO
sftp-connector         | 1.99.7 | org.apache.sqoop.connector.sftp.SftpConnector               | TO

### sqoop在hadoop家族的地位  
![sqoop在hadoop中的地位](https://github.com/mumuhadoop/mumu-sqoop/raw/master/doc/images/hadoopfamily.png?raw=true)

### sqoop工作原理
![sqoop工作原理](https://github.com/mumuhadoop/mumu-sqoop/raw/master/doc/images/sqoop.png?raw=true)

## 注意事项
创建SqoopClient的时候传递serverUrl要以'/'号结尾，要不然报错
```
Exception: org.apache.sqoop.common.SqoopException Message: CLIENT_0004:Unable to find valid Kerberos ticket cache (kinit)
```

如果以root用户开启sqoop2服务则需要添加hadoop的root代理登录，如果是其他账号同理,[查看更多](http://sqoop.apache.org/docs/1.99.7/admin/Installation.html)。
修改core-site.xml 添加如下属性
```
<configuration>
       <property>
            <name>hadoop.tmp.dir</name>
            <value>/opt/hadoop</value>
        </property>
        <property>
              <name>hadoop.proxyuser.root.hosts</name>
              <value>*</value>
        </property>
    </configuration>
```


修改container-executor.cfg 添加如下属性
```
allowed.system.users=root
```


## 相关阅读
[hadoop官网文档](http://hadoop.apache.org)

[hadoop sqoop官方文档](http://sqoop.apache.org/docs/1.99.7/index.html)

## 联系方式

以上观点纯属个人看法，如有不同，欢迎指正。

email:<babymm@aliyun.com>

github:[https://github.com/babymm](https://github.com/babymm)