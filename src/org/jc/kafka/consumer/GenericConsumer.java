/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 *
 * @author Jorge Cespedes
 * This is mostly based on the implementation available at
 * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 */
public abstract class GenericConsumer<T, V> {
    
    private static final int MAX_PARTITION_FETCH_SIZE = 10000;
    
    private static final int MAX_FETCHING_ERRORS = 5;
    
    private int partitionToConsume;
    
    private SimpleConsumer consumer;
    
    private String consumerGroup;
    
    private String topic;
    
    private String[] seedBrokers;
    
    private List<String> replicaBrokers;
    
    private String clientName;
    
    private int port;
    
    private int soTimeOut;
    
    private int maxReadRetries;
    
    public GenericConsumer( String consumerGroup,
                            String topic,
                            int partitionToConsume,
                            String[] seedBrokers,
                            int soTimeout,
                            int port,
                            int maxEmptyPartitionReadTries) {
        
            
        
        this.partitionToConsume = partitionToConsume;
        this.clientName = "Consumer" + this.partitionToConsume;
        this.soTimeOut = soTimeout;
        this.port = port;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.seedBrokers = seedBrokers;
        this.replicaBrokers = new ArrayList<>();
        this.maxReadRetries = maxEmptyPartitionReadTries;
    }
    
    public long getLastOffset(long whichTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(this.topic, this.partitionToConsume);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = 
                new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
 
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(this.topic, this.partitionToConsume));
            return 0;
        }
        long[] offsets = response.offsets(this.topic, this.partitionToConsume);
        return offsets[0];
    }
    
    private final PartitionMetadata findLeader() {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : this.seedBrokers) {
            this.consumer = null;
            try {
                this.consumer = 
                    new SimpleConsumer(seed, port, this.soTimeOut, 0, this.clientName);
 
                List<String> topics = Collections.singletonList(this.topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
 
                List<TopicMetadata> metaData = resp.topicsMetadata();
 
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == this.partitionToConsume) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + this.topic
                        + ", " + this.partitionToConsume + "] Reason: " + e);
            } finally {
                if (this.consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            this.replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                this.replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
    
    private String findNewLeader(String oldLeader) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
               // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Logger.getLogger(GenericConsumer.class.getName()).log(Level.SEVERE, null, ie);
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
    
    public final void consume() {
        this.consumePlaceholder();
    }
    
    private void consumePlaceholder() {
        PartitionMetadata metadata = this.findLeader();
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        
        String leadBroker = metadata.leader().host();
        this.consumer = 
                new SimpleConsumer(leadBroker, this.port, this.soTimeOut, 64 * 1024, this.clientName);
        
        long readOffset = 
                getLastOffset(kafka.api.OffsetRequest.EarliestTime());
        
        int numErrors = 0;
        while (this.maxReadRetries > 0) {
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(this.clientName)
                    .addFetch(this.topic, this.partitionToConsume, readOffset, MAX_PARTITION_FETCH_SIZE)
                    .build();
            
            FetchResponse fetchResponse = this.consumer.fetch(req);
            if (fetchResponse.hasError()) {
                short errorCode = 
                        fetchResponse.errorCode(this.topic, this.partitionToConsume);
                System.out.print(
                        "Fetch encountered an Error and it is: " + 
                                ErrorMapping.exceptionFor(errorCode).toString());
                if ((++numErrors) % MAX_FETCHING_ERRORS == 0) break;
                
                if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
                    readOffset = getLastOffset(OffsetRequest.LatestTime());
                    continue;
                } else if (errorCode == ErrorMapping.LeaderNotAvailableCode()) {
                    try {
                        leadBroker = this.findNewLeader(leadBroker);
                    } catch (Exception ex) {
                        Logger.getLogger(GenericConsumer.class.getName()).log(Level.SEVERE, null, ex);
                        System.out.println("Finishing consumer for partition: " + this.partitionToConsume);
                        break;
                    }
                }
                this.consumer.close();
                this.consumer = null;
            }
            
            numErrors = 0;
            boolean readMessage = false;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(this.topic, this.partitionToConsume)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                this.processConsumedMessage(messageAndOffset);
                readOffset = messageAndOffset.nextOffset();
                readMessage = true;
            }
            
            if (!readMessage) {
                --this.maxReadRetries;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Logger.getLogger(GenericConsumer.class.getName()).log(Level.SEVERE, null, ie);
                }
            }
        }
        
        if (this.consumer != null)
            this.consumer.close();
    }
    
    public void processConsumedMessage(MessageAndOffset messageAndOffset){}
}
