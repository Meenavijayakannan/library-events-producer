package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.library.Book;
import com.learnkafka.domain.library.LibraryEvent;
import com.learnkafka.domain.library.LibraryEventType;
import com.learnkafka.producer.LibraryEventsProducer;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Autowired
    ObjectMapper objectMapper;


    @Test
    void postLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Meena")
                .bookName("Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(142)
                .libraryEventType(LibraryEventType.NEW)
                .book(null)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);
        String expectedErrorMessage = "book - must not be null";
        doNothing().when(libraryEventsProducer).sendLibraryEventUsingSendMethod(isA(LibraryEvent.class));
        mockMvc.perform(post("/v1/libraryevent")
        .content(json)
        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
        .andExpect(content().string(expectedErrorMessage));
    }

    @Test
    void putLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Meena")
                .bookName("Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(142)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventsProducer.sendLibraryEventUsingSendMethod(isA(LibraryEvent.class))).thenReturn(null);
        mockMvc.perform(put("/v1/libraryevent")
                .content(json)
        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }
    @Test
    void updateLibraryEvent_withNullLibraryEventId() throws Exception {
        Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Meena")
                .bookName("Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventsProducer.sendLibraryEventUsingSendMethod(isA(LibraryEvent.class))).thenReturn(null);
        mockMvc.perform(put("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("Id is required"));

    }

}
