package com.privat.dataenrichmentservice.messaging.dto;

import java.util.UUID;


public record ResultMessage(long logId, UUID messageId, boolean result) {}
