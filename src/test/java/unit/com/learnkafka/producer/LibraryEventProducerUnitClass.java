package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.library.Book;
import com.learnkafka.domain.library.LibraryEvent;
import com.learnkafka.domain.library.LibraryEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitClass {

    @Mock
    KafkaTemplate<Integer,String> kafkaTemplate;

    @InjectMocks
    LibraryEventsProducer libraryEventsProducer;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void  sendMethod() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Meena")
                .bookName("Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(142)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception occured in calling kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);


        assertThrows(Exception.class,()->libraryEventsProducer.sendLibraryEventUsingSendMethod(libraryEvent).get());
    }

    @Test
    void  sendMethodSuccess() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Meena")
                .bookName("Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(142)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        ProducerRecord<Integer,String> producerRecord = new ProducerRecord<>
                ("library-events",libraryEvent.getLibraryEventId(),objectMapper.writeValueAsString(libraryEvent));
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events",1),1,1,342,System.currentTimeMillis(),1,1);
        SendResult<Integer,String> sendResult = new SendResult<Integer,String>(producerRecord,recordMetadata);
        future.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventsProducer.sendLibraryEventUsingSendMethod(libraryEvent);
        SendResult<Integer,String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition() == 1;

    }

}
