package com.cashfree.killbill.billing.kafkaconsumerplugin;

import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.plugin.api.notification.PluginTenantConfigurableConfigurationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaConsumerConfigurationHandler extends PluginTenantConfigurableConfigurationHandler<Properties> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfigurationHandler.class);
    private final String region;

    public KafkaConsumerConfigurationHandler(final String region,
                                          final String pluginName,
                                          final OSGIKillbillAPI osgiKillbillAPI) {
        super(pluginName, osgiKillbillAPI);
        this.region = region;
    }

    @Override
    protected Properties createConfigurable(final Properties properties) {
        logger.info("New properties for region {}: {}", region, properties);
        return properties;
    }
}


