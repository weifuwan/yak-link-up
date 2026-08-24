package com.link.up.framework.execution;

import com.link.up.api.sink.CommitScope;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.PipelineResult;
import com.link.up.framework.job.PipelineStatus;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class JobCommitSummaryMergerTest {

    @Test
    public void shouldAggregatePipelineCommitEvidence() {
        CommitSummary first =
                new CommitSummary(
                        2,
                        2,
                        1,
                        0,
                        1,
                        100L,
                        90L,
                        90L,
                        5L,
                        5L,
                        CommitScope.TASK_LOCAL,
                        "first");

        CommitSummary second =
                new CommitSummary(
                        1,
                        1,
                        1,
                        1,
                        0,
                        20L,
                        0L,
                        0L,
                        0L,
                        0L,
                        CommitScope.TASK_LOCAL,
                        "second");

        CommitSummary merged =
                JobCommitSummaryMerger.merge(
                        Arrays.asList(
                                pipeline(
                                        "pipeline-a",
                                        first),
                                pipeline(
                                        "pipeline-b",
                                        second)));

        assertEquals(3, merged.getTotalTaskCount());
        assertEquals(3, merged.getFinishedTaskCount());
        assertEquals(2, merged.getCommittedTaskCount());
        assertEquals(1, merged.getEmptyCommittedTaskCount());
        assertEquals(1, merged.getFailedOrUncommittedTaskCount());
        assertEquals(120L, merged.getAttemptedRecordCount());
        assertEquals(90L, merged.getSuccessfullyWrittenRecordCount());
        assertEquals(90L, merged.getSuccessfullyCommittedRecordCount());
        assertEquals(5L, merged.getFailedRecordCount());
        assertEquals(5L, merged.getUnknownStateRecordCount());
    }

    private static PipelineResult pipeline(
            String pipelineId,
            CommitSummary summary) {

        return new PipelineResult(
                pipelineId,
                "db.table",
                PipelineStatus.SUCCEEDED,
                summary,
                null);
    }
}
