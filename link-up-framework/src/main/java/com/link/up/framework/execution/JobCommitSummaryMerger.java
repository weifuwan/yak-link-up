package com.link.up.framework.execution;

import com.link.up.api.sink.CommitScope;
import com.link.up.framework.job.CommitSummary;
import com.link.up.framework.job.PipelineResult;

import java.util.List;
import java.util.Objects;

/** Aggregates pipeline commit observations into one job-level summary. */
final class JobCommitSummaryMerger {

    private static final String RETRY_ADVICE =
            "This sink commits per task; inspect unknown batch states before retrying.";

    private JobCommitSummaryMerger() {
    }

    static CommitSummary merge(List<PipelineResult> results) {
        Objects.requireNonNull(
                results,
                "results must not be null");

        int total = 0;
        int finished = 0;
        int committed = 0;
        int empty = 0;
        int failed = 0;
        long attempted = 0L;
        long written = 0L;
        long committedRecords = 0L;
        long failedRecords = 0L;
        long unknown = 0L;

        for (PipelineResult result : results) {
            CommitSummary summary =
                    Objects.requireNonNull(
                            result,
                            "results must not contain null values")
                            .getCommitSummary();

            total += summary.getTotalTaskCount();
            finished += summary.getFinishedTaskCount();
            committed += summary.getCommittedTaskCount();
            empty += summary.getEmptyCommittedTaskCount();
            failed += summary.getFailedOrUncommittedTaskCount();
            attempted += summary.getAttemptedRecordCount();
            written += summary.getSuccessfullyWrittenRecordCount();
            committedRecords +=
                    summary.getSuccessfullyCommittedRecordCount();
            failedRecords += summary.getFailedRecordCount();
            unknown += summary.getUnknownStateRecordCount();
        }

        return new CommitSummary(
                total,
                finished,
                committed,
                empty,
                failed,
                attempted,
                written,
                committedRecords,
                failedRecords,
                unknown,
                CommitScope.TASK_LOCAL,
                RETRY_ADVICE);
    }
}
