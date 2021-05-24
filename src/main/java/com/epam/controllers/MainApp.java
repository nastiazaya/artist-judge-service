package com.epam.controllers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * @author Evgeny Borisov
 */
@SpringBootApplication
public class MainApp {

    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(
                new SparkConf().setAppName("music").setMaster("local[*]"));
    }




    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MainApp.class);
    }
}
