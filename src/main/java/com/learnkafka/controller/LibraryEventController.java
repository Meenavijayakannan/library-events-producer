package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.library.LibraryEvent;
import com.learnkafka.domain.library.LibraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, TimeoutException, ExecutionException, InterruptedException {
        log.info("Before library Event");
        //SendResult<Integer, String> sendResult = libraryEventsProducer.sendLibraryEventSynchronously(libraryEvent);
        //libraryEventsProducer.sendLibraryEvent(libraryEvent);
        //log.info("Send result is {}",sendResult);
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventsProducer.sendLibraryEventUsingSendMethod(libraryEvent);
        log.info("After library Event");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, TimeoutException, ExecutionException, InterruptedException {
        log.info("Before library Event");
        if(libraryEvent.getLibraryEventId() == null){
           return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("please send a payload with id");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventsProducer.sendLibraryEventUsingSendMethod(libraryEvent);
        log.info("After library Event");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
