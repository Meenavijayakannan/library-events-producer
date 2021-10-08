package com.learnkafka.domain.library;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
//@Entity
public class Book {
    @NotNull
    private Integer bookId;
    @NotBlank
    private String bookName;
    @NotBlank
    private String bookAuthor;

}
