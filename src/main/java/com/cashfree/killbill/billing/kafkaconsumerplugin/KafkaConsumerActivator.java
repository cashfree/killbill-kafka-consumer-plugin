package com.cashfree.killbill.billing.kafkaconsumerplugin;

import java.util.Hashtable;
import java.util.Properties;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;

import com.cashfree.killbill.billing.kafkaconsumerplugin.config.KafkaConsumerConfig;
import com.cashfree.killbill.billing.kafkaconsumerplugin.config.KafkaProducerConfig;
import com.cashfree.killbill.billing.kafkaconsumerplugin.json.KafkaProp;
import com.cashfree.killbill.billing.kafkaconsumerplugin.listeners.UsageMetricListener;
import lombok.extern.slf4j.Slf4j;
import com.cashfree.killbill.billing.kafkaconsumerplugin.controllers.KafkaProducerServlet;
import org.killbill.billing.osgi.api.OSGIPluginProperties;
import org.killbill.billing.osgi.libs.killbill.KillbillActivatorBase;
import org.killbill.billing.plugin.core.config.PluginEnvironmentConfig;
import org.killbill.billing.plugin.core.resources.jooby.PluginApp;
import org.killbill.billing.plugin.core.resources.jooby.PluginAppBuilder;
import org.osgi.framework.BundleContext;

@Slf4j
public class KafkaConsumerActivator extends KillbillActivatorBase {
    public static final String PLUGIN_NAME = "kafka-consumer-plugin";
    private KafkaConsumerConfigurationHandler kafkaConsumerConfigurationHandler;

    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);
        log.info("KafkaConsumerActivator  started");
        final String region = PluginEnvironmentConfig.getRegion(configProperties.getProperties());

        kafkaConsumerConfigurationHandler = new KafkaConsumerConfigurationHandler(region, PLUGIN_NAME, killbillAPI);
        final Properties globalConfiguration = kafkaConsumerConfigurationHandler
                .createConfigurable(configProperties.getProperties());
        kafkaConsumerConfigurationHandler.setDefaultConfigurable(globalConfiguration);

        final KafkaProp kafkaProp = new KafkaProp(globalConfiguration);
        final KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig(kafkaProp);
        final KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig((kafkaProp));
        final UsageMetricListener kafkaListener = new UsageMetricListener(kafkaProp.getUsageMetricTopic(),killbillAPI, kafkaConsumerConfig.getKafkaConfig());

        final PluginApp pluginApp = new PluginAppBuilder(PLUGIN_NAME, killbillAPI, dataSource, super.clock,
                                                         configProperties).withRouteClass(KafkaProducerServlet.class)
                                                                          .withService(kafkaProducerConfig.getKafkaProperties()).build();
        final HttpServlet httpServlet = PluginApp.createServlet(pluginApp);
        registerServlet(context, httpServlet);

        //start kafka listeners
        new Thread(kafkaListener).start();


    }

    private void registerServlet(final BundleContext context, final Servlet servlet) {
        final Hashtable<String, String> props = new Hashtable<String, String>();
        props.put(OSGIPluginProperties.PLUGIN_NAME_PROP, PLUGIN_NAME);
        registrar.registerService(context, Servlet.class, servlet, props);
    }

    @Override
    public void stop(final BundleContext context) throws Exception {
        // Do additional work on shutdown (optional)
        super.stop(context);
        log.info("KafkaConsumerActivator stopped");


    }
}
