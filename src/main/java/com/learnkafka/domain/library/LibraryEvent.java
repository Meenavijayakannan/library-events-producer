package com.learnkafka.domain.library;



import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
//@Entity
public class LibraryEvent {

    private Integer libraryEventId;
    private LibraryEventType libraryEventType;
    @NotNull
    @Valid
    private Book book;
}
