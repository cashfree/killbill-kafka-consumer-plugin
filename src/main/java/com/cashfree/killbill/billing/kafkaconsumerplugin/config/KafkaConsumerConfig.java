package com.cashfree.killbill.billing.kafkaconsumerplugin.config;

import java.util.Properties;

import javax.inject.Inject;

import com.cashfree.killbill.billing.kafkaconsumerplugin.json.KafkaProp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerConfig {
    final Properties props = new Properties();

    @Inject
    public KafkaConsumerConfig(final KafkaProp kafkaProp){
        configureKafkaProperties(kafkaProp);

    }

    public Properties getKafkaConfig(){
        return this.props;
    }

    private void configureKafkaProperties(final KafkaProp kafkaProp){
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProp.getServer());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProp.getConsumerGroup());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        setSSLConfigProperties(props,kafkaProp);
    }
    private void setSSLConfigProperties(Properties props, final KafkaProp kafkaProp) {
        if (kafkaProp.isSslEnabled()) {
            props.put("security.protocol", "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaProp.getTrustStoreLocation());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaProp.getTrustStorePassword());
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, kafkaProp.getKeyPassword());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, kafkaProp.getKeyStorePassword());
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, kafkaProp.getKeyStoreLocation());
            props.put("ssl.client.auth", "required");
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            props.put("security.inter.broker.protocol", "SSL");
        }
    }

}
