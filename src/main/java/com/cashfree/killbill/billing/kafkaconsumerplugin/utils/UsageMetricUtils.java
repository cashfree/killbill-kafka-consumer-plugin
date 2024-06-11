package com.cashfree.killbill.billing.kafkaconsumerplugin.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.cashfree.killbill.billing.kafkaconsumerplugin.json.ConsumerSubscriptionUsageRecord;
import com.cashfree.killbill.billing.kafkaconsumerplugin.json.ConsumerUnitUsageRecord;
import com.cashfree.killbill.billing.kafkaconsumerplugin.json.ConsumerUsageRecord;
import lombok.extern.slf4j.Slf4j;
import org.jooq.tools.StringUtils;
import org.killbill.billing.catalog.api.Tier;
import org.killbill.billing.catalog.api.TieredBlock;
import org.killbill.billing.catalog.api.Usage;
import org.killbill.billing.entitlement.api.Subscription;
import com.cashfree.killbill.billing.kafkaconsumerplugin.exceptions.BadRequestException;
import org.killbill.billing.usage.api.SubscriptionUsageRecord;
import org.killbill.billing.usage.api.UnitUsageRecord;
import org.killbill.billing.usage.api.UsageRecord;

import static com.cashfree.killbill.billing.kafkaconsumerplugin.constants.CommonConstants.USAGE_EMPTY_FOR_UNIT;
import static com.cashfree.killbill.billing.kafkaconsumerplugin.constants.CommonConstants.WRONG_UNIT_PROVIDED;

@Slf4j
public class UsageMetricUtils {
    public SubscriptionUsageRecord createSubscriptionUsageRecord(final ConsumerSubscriptionUsageRecord usageRecord, final UUID subId){
        return new SubscriptionUsageRecord(subId,usageRecord.getTrackingId(),mapUnitUsageRecords(usageRecord.getUnitUsageRecords()));
    }
    private List<UnitUsageRecord> mapUnitUsageRecords(List<ConsumerUnitUsageRecord> unitUsageRecords){
        if(Objects.isNull(unitUsageRecords)) return List.of();
        final List<UnitUsageRecord> unitUsageRecordList = new ArrayList<>();
        for(final ConsumerUnitUsageRecord unitUsageRecord : unitUsageRecords){
            unitUsageRecordList.add(new UnitUsageRecord(unitUsageRecord.getUnitType(), mapUsageRecords(unitUsageRecord.getUsageRecords())));
        }
        return unitUsageRecordList;
    }
    private List<UsageRecord> mapUsageRecords(final Iterable<ConsumerUsageRecord> usageRecords){
        final List<UsageRecord> usageRecordList = new ArrayList<>();
        for(final ConsumerUsageRecord usageRecord : usageRecords){
            usageRecordList.add(new UsageRecord(usageRecord.getRecordDate(), usageRecord.getAmount()));
        }
        return usageRecordList;
    }

    public static void validateUnitUsage(ConsumerSubscriptionUsageRecord usageRecord, Subscription subscription) throws BadRequestException {
        List<ConsumerUnitUsageRecord> providedUnitUsage = usageRecord.getUnitUsageRecords();
        Usage[] usages = subscription.getLastActivePlan().getFinalPhase().getUsages();
        Set<String> unitsInActivePlan = new HashSet<>();
        for(Usage usage: usages){
            for(Tier tier : usage.getTiers()){
                for(TieredBlock tieredBlock : tier.getTieredBlocks()){
                    unitsInActivePlan.add(tieredBlock.getUnit().getName());
                }
            }
        }
        validateProvideUnitWithActivePlanUsage(providedUnitUsage, unitsInActivePlan);
    }

    public static void  validateProvideUnitWithActivePlanUsage(List<ConsumerUnitUsageRecord> consumerUnitUsageRecords,
                                                                 Set<String> unitsInActivePlan) throws BadRequestException {
        for(ConsumerUnitUsageRecord consumerUnitUsageRecord : consumerUnitUsageRecords){
            String unitProvided = consumerUnitUsageRecord.getUnitType();
            if(!unitsInActivePlan.contains(unitProvided)){
                throw new BadRequestException(String.format(WRONG_UNIT_PROVIDED,unitProvided));
            }
        }
    }




    public static void validateUsage(ConsumerSubscriptionUsageRecord usageRecord) throws BadRequestException {
        if(StringUtils.isBlank(usageRecord.getSubscriptionExternalKey())) {
            throw new BadRequestException("Subscription Id is null or empty");
        }


        if(Objects.isNull(usageRecord.getTenantId())){
            throw new BadRequestException("Tenant Id is null for subscriptionId : "
                                          + usageRecord.getSubscriptionExternalKey());
        }

        List<ConsumerUnitUsageRecord> unitUsageRecords = usageRecord.getUnitUsageRecords();
        if(Objects.isNull(unitUsageRecords) || unitUsageRecords.isEmpty()){
            throw new BadRequestException("unitUsageRecord is null or empty for subscriptionId : "
                                          + usageRecord.getSubscriptionExternalKey());
        }

        for(ConsumerUnitUsageRecord consumerUnitUsageRecord : unitUsageRecords){
            if(StringUtils.isBlank(consumerUnitUsageRecord.getUnitType()) ||
               Objects.isNull(consumerUnitUsageRecord.getUsageRecords()) ){
                throw new BadRequestException("Either unitType or usage record is null or empty for subscriptionId : "
                                              + usageRecord.getSubscriptionExternalKey());
            }
            if(consumerUnitUsageRecord.getUsageRecords().isEmpty()){
                throw new BadRequestException("Usage Record is empty  for subscriptionId : "
                                              + usageRecord.getSubscriptionExternalKey());
            }
            for(ConsumerUsageRecord unitUsageRecord : consumerUnitUsageRecord.getUsageRecords()){
                if(Objects.isNull(unitUsageRecord.getRecordDate()) || Objects.isNull(unitUsageRecord.getAmount())){
                    throw new BadRequestException(String.format(USAGE_EMPTY_FOR_UNIT, consumerUnitUsageRecord.getUnitType(),
                                                                usageRecord.getSubscriptionExternalKey()));
                }

            }
        }
    }

}
