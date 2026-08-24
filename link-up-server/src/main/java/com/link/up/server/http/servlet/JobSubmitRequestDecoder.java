package com.link.up.server.http.servlet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.link.up.server.dto.JobSubmitRequest;
import com.link.up.server.http.JsonSupport;

/** Decodes the JSON transport request used for submit/retry endpoints. */
final class JobSubmitRequestDecoder {

    private JobSubmitRequestDecoder() {
    }

    static JobSubmitRequest decode(
            String body,
            String invalidMessage) {

        try {
            return JsonSupport.mapper()
                    .readValue(
                            body,
                            JobSubmitRequest.class);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException(
                    invalidMessage,
                    failure);
        }
    }
}
