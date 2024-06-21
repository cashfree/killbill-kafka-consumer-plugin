package com.cashfree.killbill.billing.kafkaconsumerplugin.json;

import java.util.Properties;

import lombok.Data;

@Data
public class KafkaProp {
    private static final String PROPERTY_PREFIX = "org.killbill.billing.plugin.kafka.";
    private String usageMetricTopic;
    private String server;
    private String consumerGroup;
    private boolean sslEnabled;
    private String trustStoreLocation;
    private String trustStorePassword;
    private String keyPassword;
    private String keyStoreLocation;
    private String keyStorePassword;


    public KafkaProp(final Properties properties){
        this.usageMetricTopic = properties.getProperty(PROPERTY_PREFIX+"topic.usagemetric");
        this.server = properties.getProperty(PROPERTY_PREFIX+"server");
        this.consumerGroup = properties.getProperty(PROPERTY_PREFIX+"consumerGroup");
        this.sslEnabled = Boolean.parseBoolean(properties.getProperty(PROPERTY_PREFIX+"sslEnabled"));
        this.trustStoreLocation = properties.getProperty(PROPERTY_PREFIX+"trustStoreLocation");
        this.trustStorePassword = properties.getProperty(PROPERTY_PREFIX+"trustStorePassword");
        this.keyPassword = properties.getProperty(PROPERTY_PREFIX+"keyPassword");
        this.keyStoreLocation = properties.getProperty(PROPERTY_PREFIX+"keyStoreLocation");
        this.keyStorePassword = properties.getProperty(PROPERTY_PREFIX+"keyStorePassword");


    }
}
