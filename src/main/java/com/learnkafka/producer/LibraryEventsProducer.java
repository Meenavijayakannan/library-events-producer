package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.library.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    String topic="library-events";

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key=libraryEvent.getLibraryEventId();
        String value= objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
               handleFailure(key,value,ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });
    }

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventUsingSendMethod(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key=libraryEvent.getLibraryEventId();
        String value= objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer,String> producerRecord = buildProcedureRecord(topic,key,value);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key,value,ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });
        return listenableFuture;
    }

    private ProducerRecord<Integer, String> buildProcedureRecord(String topic, Integer key, String value) {
        List<Header> headers = Arrays.asList(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<Integer,String>(topic,null,key,value,headers);
    }


    public SendResult<Integer, String> sendLibraryEventSynchronously(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        Integer key=libraryEvent.getLibraryEventId();
        String value= objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult=null;

        try {
             sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            log.error("ExecutionException/InterruptedException the exception is {} ,e ");
            throw e;
        } catch (Exception e) {
            log.error("Exception the exception is {} ,e ");
            throw e;
        }
      return sendResult;
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Message got failed with exception {}",ex);
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Exception is {}" ,throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent Successfully for key {} and value {} and partition {}",key,value,result.getRecordMetadata().partition());
    }
}
