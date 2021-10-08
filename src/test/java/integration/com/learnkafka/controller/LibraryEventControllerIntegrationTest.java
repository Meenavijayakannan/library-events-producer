package com.learnkafka.controller;

import com.learnkafka.domain.library.Book;
import com.learnkafka.domain.library.LibraryEvent;
import com.learnkafka.domain.library.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"},partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    Consumer<Integer,String> consumer;

    @BeforeEach
    void setUp() {
        Map<String,Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true",embeddedKafkaBroker));
        consumer = new  DefaultKafkaConsumerFactory(configs,new IntegerDeserializer(),new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    Book book = Book.builder()
            .bookId(1)
            .bookAuthor("Meena")
            .bookName("Kafka")
            .build();

    LibraryEvent libraryEvent = LibraryEvent.builder()
            .libraryEventId(142)
            .libraryEventType(LibraryEventType.NEW)
            .book(new Book())
            .build();


    HttpEntity<LibraryEvent> request= null;



    @Test
    @Timeout(5)
    void postLibraryEvent() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type",MediaType.APPLICATION_JSON.toString());
        request=new HttpEntity<>(libraryEvent,httpHeaders);
        ResponseEntity<LibraryEvent> requestEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);
        assertEquals(HttpStatus.CREATED,requestEntity.getStatusCode());

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = consumerRecord.value();
        String expectedRecord="{\"libraryEventId\":142,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1,\"bookName\":\"Kafka\",\"bookAuthor\":\"Meena\"}}";
        assertEquals(expectedRecord,value);
    }

    @Test
    @Timeout(5)
    void putLibraryEvent(){
        Book book = Book.builder()
                .bookId(123)
                .bookName("Pragmatic code school")
                .bookAuthor("dilip")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .book(book)
                .libraryEventId(456)
                .libraryEventType(LibraryEventType.UPDATE)
                .build();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type",MediaType.APPLICATION_JSON.toString());
        HttpEntity httpEntity = new HttpEntity(libraryEvent , httpHeaders);
        ResponseEntity<LibraryEvent> entity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, httpEntity, LibraryEvent.class);
        assertEquals(HttpStatus.OK,entity.getStatusCode());

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = consumerRecord.value();
        String expectedValue = "{\"libraryEventId\":456,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Pragmatic code school\",\"bookAuthor\":\"dilip\"}}";
        assertEquals(value,expectedValue);


    }

}
