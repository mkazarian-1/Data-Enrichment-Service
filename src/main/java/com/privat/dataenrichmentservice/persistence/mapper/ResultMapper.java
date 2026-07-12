package com.privat.dataenrichmentservice.persistence.mapper;

import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.persistence.ResultEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface ResultMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "messageId", source = "message.messageId")
    @Mapping(target = "userId", source = "message.userId")
    @Mapping(target = "action", source = "message.action")
    @Mapping(target = "result", source = "enrichment.result")
    ResultEntity toEntity(IncomingMessage message, EnrichmentResponse enrichment);
}
