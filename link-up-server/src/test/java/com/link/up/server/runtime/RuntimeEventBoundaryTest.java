package com.link.up.server.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobRuntimeEvent;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Guards the offline-only lifecycle-event boundary. */
public class RuntimeEventBoundaryTest {

    @Test
    public void eventVocabularyMustStayAtJobAttemptLifecycleLevel() {
        List<JobRuntimeEventType> expected = Arrays.asList(
                JobRuntimeEventType.JOB_SUBMITTED,
                JobRuntimeEventType.JOB_QUEUED,
                JobRuntimeEventType.JOB_STARTED,
                JobRuntimeEventType.JOB_LOG_CREATED,
                JobRuntimeEventType.JOB_CANCEL_REQUESTED,
                JobRuntimeEventType.JOB_RETRY_CREATED,
                JobRuntimeEventType.JOB_SUCCEEDED,
                JobRuntimeEventType.JOB_FAILED,
                JobRuntimeEventType.JOB_CANCELED,
                JobRuntimeEventType.JOB_LOST);

        assertEquals(
                expected,
                Arrays.asList(JobRuntimeEventType.values()));

        for (JobRuntimeEventType type : JobRuntimeEventType.values()) {
            assertTrue(type.name().startsWith("JOB_"));
            assertFalse(type.name().startsWith("TASK_"));
            assertFalse(type.name().startsWith("SPLIT_"));
            assertFalse(type.name().startsWith("BATCH_"));
        }
    }

    @Test
    public void newEventsMustNotSerializeExecutionSnapshots()
            throws Exception {

        JobEventEnvelope envelope = JobEventEnvelope.create(
                "job-1",
                "job-1-attempt-1",
                1,
                1L,
                1L,
                JobRuntimeEvent.terminal(
                        JobRuntimeEventType.JOB_FAILED,
                        ServerJobStatus.RUNNING,
                        ServerJobStatus.FAILED,
                        "job-failed",
                        "SQLException"));

        String json = new ObjectMapper()
                .writeValueAsString(envelope);

        assertEquals(2, envelope.getSchemaVersion());
        assertFalse(json.contains("\"execution\""));
        assertFalse(json.contains("pipelines"));
        assertFalse(json.contains("tasks"));
    }

    @Test
    public void legacyV1ExecutionPayloadMustRemainReadable()
            throws Exception {

        String legacyJson = "{"
                + "\"schemaVersion\":1,"
                + "\"eventId\":\"job-1:1\","
                + "\"jobId\":\"job-1\","
                + "\"attemptId\":\"job-1-attempt-1\","
                + "\"attemptNumber\":1,"
                + "\"sequence\":1,"
                + "\"occurredAtMillis\":1,"
                + "\"event\":{"
                + "\"type\":\"JOB_FAILED\","
                + "\"previousStatus\":\"RUNNING\","
                + "\"status\":\"FAILED\","
                + "\"reason\":\"job-failed\","
                + "\"failureType\":\"SQLException\","
                + "\"execution\":{\"pipelines\":[],\"tasks\":[]}"
                + "}}";

        JobEventEnvelope restored = new ObjectMapper()
                .readValue(
                        legacyJson,
                        JobEventEnvelope.class);

        assertEquals(1, restored.getSchemaVersion());
        assertEquals(
                JobRuntimeEventType.JOB_FAILED,
                restored.getEvent().getType());
    }
}
