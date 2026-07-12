package com.privat.dataenrichmentservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DataEnrichmentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataEnrichmentServiceApplication.class, args);
    }
}
