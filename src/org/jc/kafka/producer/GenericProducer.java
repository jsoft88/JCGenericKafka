/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kafka.producer;

import java.util.List;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author cespedjo
 * @param <T> KeyedMessage Type
 * @param <V> KeyedMessage Type
 * @param <K> Class of Partition Key. Default is null so be careful.
 * @param <L> Class of the object from which a partition key for keyedMessage 
 * will be extracted from. For instances, this could be a json string. 
 */
public abstract class GenericProducer<T, V, K, L> {
    
    private kafka.javaapi.producer.Producer<T, V> producer;
    
    public GenericProducer(ProducerConfig config) {
        this.producer = new kafka.javaapi.producer.Producer<>(config);
    }
    
    /**
     * Method that will be invoked so that producers can start producing.
     */
    public void produce(){}
    
    /**
     * Method to post a single message into a topic
     * @param message the message to be posted under a topic.
     */
    public void sendMessage(KeyedMessage<T,V> message){
        producer.send(message);
    }
    
    /**
     * Method to post messages in batch.
     * @param messages A List of messages to be posted under a topic.
     */
    public void sendMessageBatch(List<KeyedMessage<T,V>> messages) {
        producer.send(messages);
    }
    
    /**
     * Given an object of class L, find the key K.
     * @param haystack this could be a record and message key is searched inside
     * the object.
     * @return the value of the found key.
     */
    public K getMessageKey(L haystack){ return null;}
}
