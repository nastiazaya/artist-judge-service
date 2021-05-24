package com.epam.controllers;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author Evgeny Borisov
 */

@RestController
@RequestMapping("/api")
public class HelloWorldJavaController {

    @Autowired
    private JavaSparkContext sc;

    @GetMapping("/hello")
    public String hello() {

        JavaRDD<Integer> rdd = sc.parallelize(List.of(1, 2, 3));


        return "Hello from JAVA "+rdd.count();
    }

    @GetMapping("/hello2")
    public String helloWithNameFromRequest(@RequestParam String name) {
        return "Hello from JAVA " + name.toUpperCase();
    }

    @GetMapping("/hello/{name}")
    public String helloWithNameFromPath(@PathVariable String name) {
        return "Hello from JAVA " + name.toUpperCase();
    }


    @PostMapping("/person")
    public Person updatePerson(@RequestBody Person person) {
        return person.withAge(person.getAge() + 10).withName(person.getName().toUpperCase());
    }


}






