package com.yqy.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

/**
 * @author bahsk
 * @createTime 2021-01-31 22:48
 * @description
 */
public class AdminSample {

     /**
      * @author: bahsk
      * @date: 2021/1/31 22:57
      * @description: 设置AdminClient
      * @params:
      * @return:
      */
    public static AdminClient adminClient() {

        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"47.114.63.55:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }
}
