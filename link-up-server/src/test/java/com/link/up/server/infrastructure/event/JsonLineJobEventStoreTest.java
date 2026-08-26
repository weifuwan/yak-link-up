package com.link.up.server.infrastructure.event;

import com.link.up.server.runtime.ServerJobStatus;
import com.link.up.server.runtime.event.JobEventEnvelope;
import com.link.up.server.runtime.event.JobEventPage;
import com.link.up.server.runtime.event.JobRuntimeEvent;
import com.link.up.server.runtime.event.JobRuntimeEventType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonLineJobEventStoreTest {

    @Rule
    public TemporaryFolder temporaryFolder =
            new TemporaryFolder();

    @Test
    public void shouldAppendPageAndResumeAfterRestart()
            throws Exception {

        Path stateDirectory =
                temporaryFolder.newFolder("state")
                        .toPath();
        JsonLineJobEventStore store =
                new JsonLineJobEventStore(stateDirectory);

        store.onEvent(event(1L, JobRuntimeEventType.JOB_SUBMITTED));
        store.onEvent(event(2L, JobRuntimeEventType.JOB_QUEUED));
        store.onEvent(event(2L, JobRuntimeEventType.JOB_QUEUED));

        JobEventPage first = store.read("job-1", 0L, 1);
        assertEquals(1, first.getItems().size());
        assertEquals(1L, first.getNextSequence());
        assertTrue(first.isHasMore());

        JobEventPage second = store.read("job-1", 1L, 10);
        assertEquals(1, second.getItems().size());
        assertEquals(2L, second.getNextSequence());
        assertFalse(second.isHasMore());

        JsonLineJobEventStore restarted =
                new JsonLineJobEventStore(stateDirectory);
        restarted.onEvent(event(3L, JobRuntimeEventType.JOB_STARTED));

        JobEventPage all = restarted.read("job-1", 0L, 10);
        assertEquals(3, all.getItems().size());
        assertEquals(3L, all.getNextSequence());

        List<String> lines = Files.readAllLines(
                stateDirectory.resolve("job-events/job-1.jsonl"));
        assertEquals(3, lines.size());
    }

    @Test
    public void shouldDeleteJournalsOutsideRetainedHistory()
            throws Exception {

        Path stateDirectory =
                temporaryFolder.newFolder("retention")
                        .toPath();
        JsonLineJobEventStore store =
                new JsonLineJobEventStore(stateDirectory);

        store.onEvent(event(
                "job-1",
                1L,
                JobRuntimeEventType.JOB_SUBMITTED));
        store.onEvent(event(
                "job-2",
                1L,
                JobRuntimeEventType.JOB_SUBMITTED));

        store.retain(Collections.singletonList("job-1"));

        assertTrue(Files.exists(
                stateDirectory.resolve("job-events/job-1.jsonl")));
        assertFalse(Files.exists(
                stateDirectory.resolve("job-events/job-2.jsonl")));
    }

    private JobEventEnvelope event(
            long sequence,
            JobRuntimeEventType type) {

        ServerJobStatus previous =
                sequence == 1L
                        ? ServerJobStatus.CREATED
                        : sequence == 2L
                        ? ServerJobStatus.SUBMITTED
                        : ServerJobStatus.QUEUED;
        ServerJobStatus status =
                sequence == 1L
                        ? ServerJobStatus.SUBMITTED
                        : sequence == 2L
                        ? ServerJobStatus.QUEUED
                        : ServerJobStatus.RUNNING;

        return event(
                "job-1",
                sequence,
                type,
                previous,
                status);
    }

    private JobEventEnvelope event(
            String jobId,
            long sequence,
            JobRuntimeEventType type) {

        return event(
                jobId,
                sequence,
                type,
                ServerJobStatus.CREATED,
                ServerJobStatus.SUBMITTED);
    }

    private JobEventEnvelope event(
            String jobId,
            long sequence,
            JobRuntimeEventType type,
            ServerJobStatus previous,
            ServerJobStatus status) {

        return JobEventEnvelope.create(
                jobId,
                jobId + "-attempt-1",
                1,
                sequence,
                1_000L + sequence,
                JobRuntimeEvent.transition(
                        type,
                        previous,
                        status,
                        type.name().toLowerCase()));
    }
}
