package com.cashfree.killbill.billing.kafkaconsumerplugin.constants;

public class CommonConstants {
    public static final String USAGE_EMPTY_FOR_UNIT = "Either Record Date or Amount is null of unit type: %s for subscriptionId : %s";
    public static final String KAFKA_CONSUMER_PLUGIN = "KafkaConsumerPlugin";
    public static final String REASON_CODE = "Consumed message in ";
    public static final String COMMENT = "Processing by ";
    public static final String WRONG_UNIT_PROVIDED = "Wrong Unit provided :: There is not unit with name %s in the active plan to record";
}
