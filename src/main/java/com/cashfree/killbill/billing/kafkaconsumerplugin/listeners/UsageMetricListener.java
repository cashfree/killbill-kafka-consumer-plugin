package com.cashfree.killbill.billing.kafkaconsumerplugin.listeners;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import com.cashfree.killbill.billing.kafkaconsumerplugin.worker.UsageMetricWorker;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class UsageMetricListener implements Runnable, ConsumerRebalanceListener {
    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final OSGIKillbillAPI osgiKillbillAPI;
    private final Map<TopicPartition, UsageMetricWorker> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private final long POLL_INTERVAL = 100;
    private long lastCommitTime = System.currentTimeMillis();
    private final String topic;

    public UsageMetricListener(String topic, final OSGIKillbillAPI osgiKillbillAPI, Properties kafkaProperties) {
        this.osgiKillbillAPI = osgiKillbillAPI;
        consumer = new KafkaConsumer<>(kafkaProperties);
        this.topic = topic;
        new Thread(this).start();
    }
    @Override
    public void run() {
        log.info("Kafka Consumer started .....");
        try {
            consumer.subscribe(Collections.singleton(topic), this);
            while (!stopped.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(POLL_INTERVAL, ChronoUnit.MILLIS));
                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException we) {
            if (!stopped.get())
                throw we;
        } finally {
            consumer.close();
        }
    }
    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }

    private void handleFetchedRecords(ConsumerRecords<String, String> records) {
        if (records.count() > 0) {
            List<TopicPartition> partitionsToPause = new ArrayList<>();
            records.partitions().forEach(partition -> {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                UsageMetricWorker task = new UsageMetricWorker(partitionRecords,osgiKillbillAPI, partition);
                partitionsToPause.add(partition);
                executor.submit(task);
                activeTasks.put(partition, task);
            });
            consumer.pause(partitionsToPause);
        }
    }

    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, task) -> {
            if (task.isFinished())
                finishedTasksPartitions.add(partition);
            long offset = task.getCurrentOffset();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
        consumer.resume(finishedTasksPartitions);
    }


    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        // 1. Stop all tasks handling records from revoked partitions
        Map<TopicPartition, UsageMetricWorker> stoppedTask = new HashMap<>();
        for (TopicPartition partition : partitions) {
            UsageMetricWorker task = activeTasks.remove(partition);
            if (task != null) {
                task.stop();
                stoppedTask.put(partition, task);
            }
        }

        // 2. Wait for stopped tasks to complete processing of current record
        stoppedTask.forEach((partition, task) -> {
            long offset = task.waitForCompletion();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });


        // 3. collect offsets for revoked partitions
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        partitions.forEach( partition -> {
            OffsetAndMetadata offset = offsetsToCommit.remove(partition);
            if (offset != null)
                revokedPartitionOffsets.put(partition, offset);
        });

        // 4. commit offsets for revoked partitions
        try {
            consumer.commitSync(revokedPartitionOffsets);
        } catch (Exception e) {
            log.warn("Failed to commit offsets for revoked partitions!");
        }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        consumer.resume(partitions);
    }

    public void stopConsuming() {
        stopped.set(true);
        consumer.wakeup();
    }
}
