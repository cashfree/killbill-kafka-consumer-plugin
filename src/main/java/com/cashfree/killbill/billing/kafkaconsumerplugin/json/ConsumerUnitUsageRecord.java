package com.cashfree.killbill.billing.kafkaconsumerplugin.json;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ConsumerUnitUsageRecord {
    private String unitType;

    private List<ConsumerUsageRecord> usageRecords;
}
