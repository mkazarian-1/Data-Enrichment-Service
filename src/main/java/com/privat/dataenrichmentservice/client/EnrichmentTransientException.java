package com.privat.dataenrichmentservice.client;

public class EnrichmentTransientException extends RuntimeException {

    public EnrichmentTransientException(String message) {
        super(message);
    }

    public EnrichmentTransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
