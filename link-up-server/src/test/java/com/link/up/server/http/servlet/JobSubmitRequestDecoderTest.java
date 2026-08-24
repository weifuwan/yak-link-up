package com.link.up.server.http.servlet;

import com.link.up.server.dto.JobSubmitRequest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JobSubmitRequestDecoderTest {

    @Test
    public void shouldDecodeStableSubmissionFields() {
        JobSubmitRequest request =
                JobSubmitRequestDecoder.decode(
                        "{\"externalExecutionId\":\"external-1\","
                                + "\"idempotencyKey\":\"key-1\","
                                + "\"definitionVersion\":2,"
                                + "\"hocon\":\"job {}\"}",
                        "invalid");

        assertEquals(
                "external-1",
                request.getExternalExecutionId());
        assertEquals(
                "key-1",
                request.getIdempotencyKey());
        assertEquals(
                Integer.valueOf(2),
                request.getDefinitionVersion());
        assertEquals(
                "job {}",
                request.getHocon());
    }

    @Test
    public void shouldMapInvalidJsonToIllegalArgumentException() {
        try {
            JobSubmitRequestDecoder.decode(
                    "{invalid",
                    "Invalid JSON submit request");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException failure) {
            assertEquals(
                    "Invalid JSON submit request",
                    failure.getMessage());
        }
    }
}
