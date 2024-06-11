package com.cashfree.killbill.billing.kafkaconsumerplugin.worker;

import com.cashfree.killbill.billing.kafkaconsumerplugin.json.ConsumerSubscriptionUsageRecord;
import com.cashfree.killbill.billing.kafkaconsumerplugin.utils.UsageMetricUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;
import org.killbill.billing.entitlement.api.Subscription;
import org.killbill.billing.entitlement.api.SubscriptionApiException;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.usage.api.SubscriptionUsageRecord;
import org.killbill.billing.usage.api.UsageApiException;
import org.killbill.billing.util.callcontext.CallOrigin;
import org.killbill.billing.util.callcontext.UserType;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static com.cashfree.killbill.billing.kafkaconsumerplugin.constants.CommonConstants.COMMENT;
import static com.cashfree.killbill.billing.kafkaconsumerplugin.constants.CommonConstants.KAFKA_CONSUMER_PLUGIN;
import static com.cashfree.killbill.billing.kafkaconsumerplugin.constants.CommonConstants.REASON_CODE;


@Slf4j
public class UsageMetricWorker implements Runnable{
    private final List<ConsumerRecord<String, String>> records;
    private final ReentrantLock startStopLock = new ReentrantLock();
    private volatile boolean stopped = false;

    private volatile boolean started = false;

    private volatile boolean finished = false;
    private final OSGIKillbillAPI osgiKillbillAPI;
    private final ObjectMapper objectMapper ;
    private UsageMetricUtils usageMetricUtils;
    private final CompletableFuture<Long> completion = new CompletableFuture<>();
    private final TopicPartition partition;
    private final AtomicLong currentOffset = new AtomicLong();

    public UsageMetricWorker(List<ConsumerRecord<String, String>> records, OSGIKillbillAPI osgiKillbillAPI, TopicPartition partition) {
        this.records = records;
        this.osgiKillbillAPI = osgiKillbillAPI;
        this.objectMapper  = new ObjectMapper();
        objectMapper.registerModule(new JodaModule());
        this.usageMetricUtils = new UsageMetricUtils();
        this.partition = partition;
    }

@Override
public void run() {
    startStopLock.lock();
    if (stopped){
        return;
    }
    started = true;
    startStopLock.unlock();

    for (ConsumerRecord<String, String> record : records) {
        if (stopped)
            break;
        if(record==null){
            log.info("record is null");
            continue;
        }
        try{
            log.info("Received message: {} from partition : {}" , record.value(),partition.partition());
            ConsumerSubscriptionUsageRecord usageRecord = null;
            usageRecord = objectMapper.readValue(record.value(), ConsumerSubscriptionUsageRecord.class);
            log.info("UsageMetricWorker :: usageRecord :: " + usageRecord);
            UsageMetricUtils.validateUsage(usageRecord);
            final PluginCallContext callContext = new PluginCallContext(UUID.randomUUID(), KAFKA_CONSUMER_PLUGIN, CallOrigin.INTERNAL,
                    UserType.ADMIN, REASON_CODE + this.getClass().getSimpleName(), COMMENT + this.getClass().getSimpleName(),
                    DateTime.now(), DateTime.now(), null, usageRecord.getTenantId());

            Subscription subscription = osgiKillbillAPI.getSubscriptionApi().getSubscriptionForExternalKey(usageRecord.getSubscriptionExternalKey(), false, callContext);
            UsageMetricUtils.validateUnitUsage(usageRecord,subscription);
            final SubscriptionUsageRecord subscriptionUsageRecord = usageMetricUtils.createSubscriptionUsageRecord(usageRecord, subscription.getId());
            osgiKillbillAPI.getUsageUserApi().recordRolledUpUsage(subscriptionUsageRecord, callContext);
            log.info("UsageMetricWorker :: usage recorded");

        }
        catch (JsonProcessingException e) {
            log.error("UsageMetricWorker :: JsonProcessingException :: " + e.getMessage());
        }   catch (UsageApiException e) {
            log.error("UsageMetricWorker :: UsageApiException :: " + e.getMessage());
        } catch (SubscriptionApiException e) {
            log.error("UsageMetricWorker :: SubscriptionApiException :: " + e.getMessage());
        }
        catch(Exception e){
            log.error("UsageMetricWorker :: Exception :: " + e.getMessage());
        }
        currentOffset.set(record.offset() + 1);
    }
    finished = true;
    completion.complete(currentOffset.get());
}

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(currentOffset.get());
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return finished;
    }


}

