package com.privat.dataenrichmentservice.messaging.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.LocalDateTime;
import java.util.UUID;

public record IncomingMessage(
        @NotNull UUID messageId,
        @NotNull @Positive Long userId,
        @NotBlank String action,
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.S") LocalDateTime timestamp) {}
