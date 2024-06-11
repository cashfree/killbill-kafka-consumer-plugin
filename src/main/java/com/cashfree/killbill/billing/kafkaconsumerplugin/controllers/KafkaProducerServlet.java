package com.cashfree.killbill.billing.kafkaconsumerplugin.controllers;

import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;

import com.cashfree.killbill.billing.kafkaconsumerplugin.json.ConsumerSubscriptionUsageRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jooby.mvc.Body;
import org.jooby.mvc.POST;
import org.jooby.mvc.Path;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

@Slf4j
@Singleton
@Path("/")
public class KafkaProducerServlet {
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;


    @Inject
    public KafkaProducerServlet(final Properties kafkaProp) {
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JodaModule());
        this.producer = new KafkaProducer<>(kafkaProp);
    }

    @POST
    public void publishMessage(@QueryParam("topicName") @DefaultValue("") final String topicName,
                               @Body final ConsumerSubscriptionUsageRecord consumerSubscriptionUsageRecord) {
        log.info("KafkaProducerServlet :: publishMessage :: consumerSubscriptionUsageRecord :: {} ", consumerSubscriptionUsageRecord);
        try {
            String jsondata = null;
            jsondata = objectMapper.writeValueAsString(consumerSubscriptionUsageRecord);
            producer.send(new ProducerRecord<>(topicName, jsondata));
            log.info("KafkaProducerServlet :: publishMessage :: message published");
        } catch (JsonProcessingException e) {
            log.error("Parsing error :: {}", e.getMessage());
        } catch (Exception e) {
            log.error("KafkaProducerServlet :: Exception :: {} classname :: {}", e.getMessage(), e.toString());
        }

    }

}
