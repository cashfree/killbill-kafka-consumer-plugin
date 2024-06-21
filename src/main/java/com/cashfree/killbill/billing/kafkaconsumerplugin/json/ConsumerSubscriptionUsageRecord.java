package com.cashfree.killbill.billing.kafkaconsumerplugin.json;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ConsumerSubscriptionUsageRecord {
    private String subscriptionExternalKey;
    private String trackingId;
    private UUID tenantId;
    private List<ConsumerUnitUsageRecord> unitUsageRecords;
    @JsonCreator
    public ConsumerSubscriptionUsageRecord(@JsonProperty("subscriptionExternalKey") String subscriptionExternalKey,
                                           @JsonProperty("trackingId") String trackingId,
                                           @JsonProperty("tenantId") UUID tenantId,
                                           @JsonProperty("unitUsageRecords") List<ConsumerUnitUsageRecord> unitUsageRecords) {
        this.subscriptionExternalKey = subscriptionExternalKey;
        this.trackingId = trackingId;
        this.tenantId = tenantId;
        this.unitUsageRecords = unitUsageRecords;
    }
}
