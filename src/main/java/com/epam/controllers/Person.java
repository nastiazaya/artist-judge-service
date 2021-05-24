package com.epam.controllers;

import lombok.*;

/**
 * @author Evgeny Borisov
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@With
public class Person {
    private String name;
    private int age;
}
