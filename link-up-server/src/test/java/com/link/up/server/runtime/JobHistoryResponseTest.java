package com.link.up.server.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.server.domain.JobAttemptStatus;
import com.link.up.server.dto.JobHistoryResponse;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;
import com.link.up.server.runtime.event.JobRuntimeEvent;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobHistoryResponseTest {

    private static final String SECRET =
            "TEST_ONLY_SECRET";

    @Test
    public void historyProjectionMustKeepRawExecutionDetailsOutOfJson()
            throws Exception {

        JobHistoryResponse response = new JobHistoryResponse(
                snapshot(),
                metadata(),
                events());

        String json = new ObjectMapper()
                .writeValueAsString(response);

        assertEquals(
                JobHistoryResponse.CURRENT_API_VERSION,
                response.getApiVersion());
        assertEquals("job-history-1", response.getJobId());
        assertTrue(response.isCompleted());
        assertEquals(1, response.getAttempts().size());
        assertEquals(1, response.getPipelines().size());
        assertEquals(1, response.getPipelines().get(0).getTasks().size());
        assertEquals(1, response.getPipelines().get(0).getChannels().size());
        assertEquals("PLAN-006",
                response.getAttempts().get(0).getErrorCode());
        assertTrue(json.contains("PLAN-006"));
        assertTrue(json.contains("pipeline-demo.orders"));

        assertFalse(json.contains(SECRET));
        assertFalse(json.contains("jdbc:mysql://"));
        assertFalse(json.contains("SELECT * FROM secret_table"));
        assertFalse(json.contains("jobLogFile"));
        assertFalse(json.contains("failureMessage"));
        assertFalse(json.contains("retryAdvice"));
        assertFalse(json.contains("currentTable"));
        assertFalse(json.contains("currentSplit"));
        assertFalse(json.contains("errorMessage"));
    }

    private static JobSnapshot snapshot() {
        JobSnapshot.Metrics metrics = new JobSnapshot.Metrics(
                4L, 100L, 1024L, 20D,
                4L, 100L, 98L, 900L, 19D,
                2L, 0L, 0L, 1L,
                2L, 2L, 0L, 0L, 0L,
                8L, 10L,
                0.1D, 0.2D, 0D);

        JobSnapshot.Commit commit = new JobSnapshot.Commit(
                2, 2, 2, 2, 0, 0,
                100L, 98L, 98L, 2L, 0L,
                false, false,
                "TASK_LOCAL",
                "password=" + SECRET);

        JobSnapshot.Task task = new JobSnapshot.Task(
                "pipeline-demo.orders-source-0",
                "pipeline-demo.orders",
                "SOURCE",
                "FINISHED",
                0,
                1,
                4L,
                0L,
                100L,
                100L,
                0L,
                0L,
                2L,
                20D,
                1000L,
                "jdbc:mysql://localhost/demo?password=" + SECRET,
                "SELECT * FROM secret_table");

        JobSnapshot.Channel channel = new JobSnapshot.Channel(
                "pipeline-demo.orders-channel-0",
                4L,
                4L,
                0L,
                0L,
                0L,
                32L,
                1000L,
                1024L,
                0L,
                0.1D,
                0.2D,
                0D);

        JobSnapshot.Source source = new JobSnapshot.Source(
                "jdbc",
                "jdbc:mysql://localhost/demo?password=" + SECRET,
                1,
                4L,
                100L,
                1024L,
                2L,
                2L,
                0L,
                20D);

        JobSnapshot.Sink sink = new JobSnapshot.Sink(
                "doris",
                "password=" + SECRET,
                1,
                4L,
                100L,
                98L,
                2L,
                0L,
                900L,
                98L,
                19D);

        JobSnapshot.Pipeline pipeline = new JobSnapshot.Pipeline(
                "pipeline-demo.orders",
                "demo.orders",
                "FAILED",
                source,
                sink,
                commit,
                Collections.singletonList(task),
                Collections.singletonList(channel),
                "password=" + SECRET);

        return new JobSnapshot(
                "job-history-1",
                "history-test",
                ServerJobStatus.FAILED,
                1L,
                2L,
                3L,
                1L,
                metrics,
                commit,
                Collections.singletonList(pipeline),
                "FLUX-JOB-FAILED",
                "password=" + SECRET);
    }

    private static JobExecutionMetadata metadata() {
        JobAttemptMetadata attempt = new JobAttemptMetadata(
                1,
                "job-history-1-attempt-1",
                JobAttemptStatus.FAILED,
                1L,
                1L,
                2L,
                3L,
                "run-history-1",
                "logs/password=" + SECRET + ".log",
                "java.sql.SQLException",
                "password=" + SECRET,
                "retry with password=" + SECRET,
                "PLAN-006",
                "PREPARATION",
                "SOURCE_DISCOVERY",
                true,
                "JOB",
                true,
                0,
                0L,
                0L,
                false,
                "TASK_LOCAL");

        return new JobExecutionMetadata(
                "external-history-1",
                "key-history-1",
                1,
                "digest-history-1",
                1L,
                1L,
                5L,
                5L,
                false,
                Collections.<JobStateTransition>emptyList(),
                Collections.emptyMap(),
                "run-history-1",
                "logs/password=" + SECRET + ".log",
                Collections.singletonList(attempt));
    }

    private static JobEventPage events() {
        JobEventEnvelope event = JobEventEnvelope.create(
                "job-history-1",
                "job-history-1-attempt-1",
                1,
                1L,
                1L,
                JobRuntimeEvent.transition(
                        JobRuntimeEventType.JOB_SUBMITTED,
                        ServerJobStatus.CREATED,
                        ServerJobStatus.SUBMITTED,
                        "submission-accepted"));

        return new JobEventPage(
                "job-history-1",
                Collections.singletonList(event),
                1L,
                false);
    }
}
