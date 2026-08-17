package com.link.up.server.runtime;

import com.link.up.framework.execution.JobExecution;
import com.link.up.framework.execution.JobExecutionListener;
import com.link.up.framework.job.JobDefinition;
import com.link.up.framework.job.JobResult;
import com.link.up.framework.job.JobStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单节点离线作业 Worker 协调器。
 *
 * <p>负责幂等提交、显式队列状态、执行取消和终态归档。
 */
public final class LocalJobManager
        implements JobManager {

    private final int maxQueuedJobs;
    private final Semaphore admissionSemaphore;
    private final java.util.concurrent.ConcurrentMap<String, Thread>
            jobThreads =
            new java.util.concurrent.ConcurrentHashMap<
                    String, Thread>();
    private final AtomicInteger threadSequence =
            new AtomicInteger();
    private final JobExecutor jobExecutor;
    private final JobRepository repository;
    private final JobIdGenerator jobIdGenerator;
    private final long shutdownTimeoutMillis;

    private final Map<String, JobExecutionHandle>
            runningJobs =
            new java.util.concurrent.ConcurrentHashMap<
                    String,
                    JobExecutionHandle>();

    private final Map<String, String>
            jobIdsByIdempotencyKey =
            new java.util.concurrent.ConcurrentHashMap<
                    String,
                    String>();

    private final Map<String, String>
            jobIdsByExternalExecutionId =
            new java.util.concurrent.ConcurrentHashMap<
                    String,
                    String>();

    private final AtomicBoolean closed =
            new AtomicBoolean(false);

    public LocalJobManager(
            int jobThreads,
            int maxQueuedJobs,
            long shutdownTimeoutMillis,
            JobExecutor jobExecutor,
            JobRepository repository,
            JobIdGenerator jobIdGenerator) {

        if (jobThreads <= 0) {
            throw new IllegalArgumentException(
                    "jobThreads must be greater than 0");
        }

        if (maxQueuedJobs <= 0) {
            throw new IllegalArgumentException(
                    "maxQueuedJobs must be greater than 0");
        }

        this.maxQueuedJobs = maxQueuedJobs;
        this.jobExecutor = jobExecutor;
        this.repository = repository;
        this.jobIdGenerator = jobIdGenerator;
        this.shutdownTimeoutMillis =
                Math.max(
                        0L,
                        shutdownTimeoutMillis);

        this.admissionSemaphore =
                new Semaphore(maxQueuedJobs);
    }

    public JobSnapshot submit(
            JobDefinition definition) {

        return submit(
                JobSubmission.legacy(
                        definition));
    }

    public synchronized JobSnapshot submit(
            final JobSubmission submission) {

        ensureOpen();

        JobSnapshot existing =
                findExistingSubmission(
                        submission);

        if (existing != null) {
            return existing;
        }

        final String jobId =
                jobIdGenerator.nextId();

        final JobExecutionHandle handle =
                new JobExecutionHandle(
                        jobId,
                        submission);

        JobExecutionHandle previous =
                runningJobs.putIfAbsent(
                        jobId,
                        handle);

        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate jobId: " + jobId);
        }

        try {
            registerSubmission(
                    jobId,
                    submission);
        } catch (RuntimeException exception) {
            runningJobs.remove(
                    jobId,
                    handle);
            throw exception;
        }

        handle.markSubmitted();

        if (!admissionSemaphore.tryAcquire()) {
            runningJobs.remove(
                    jobId,
                    handle);
            unregisterSubmission(
                    jobId,
                    submission);
            throw new RejectedExecutionException(
                    "Job queue is full (maxQueuedJobs="
                            + maxQueuedJobs + ")");
        }

        handle.markQueued();

        Thread jobThread =
                new Thread(
                        new Runnable() {
                            public void run() {
                                try {
                                    executeJob(handle);
                                } finally {
                                    jobThreads.remove(jobId);
                                    admissionSemaphore.release();
                                }
                            }
                        },
                        "link-up-job-"
                                + jobId
                                + "-"
                                + threadSequence.incrementAndGet());
        jobThread.setDaemon(false);

        jobThreads.put(jobId, jobThread);
        jobThread.start();

        return JobSnapshotFactory.create(handle);
    }

    private JobSnapshot findExistingSubmission(
            JobSubmission submission) {

        String byIdempotency =
                jobIdsByIdempotencyKey.get(
                        submission.getIdempotencyKey());

        String byExternal =
                jobIdsByExternalExecutionId.get(
                        submission.getExternalExecutionId());

        if (byIdempotency == null
                && byExternal == null) {
            return null;
        }

        if (byIdempotency != null
                && byExternal != null
                && !byIdempotency.equals(byExternal)) {

            throw new JobSubmissionConflictException(
                    "idempotencyKey and externalExecutionId reference different jobs");
        }

        String jobId =
                byIdempotency != null
                        ? byIdempotency
                        : byExternal;

        JobSnapshot snapshot;

        try {
            snapshot = getJob(jobId);
        } catch (JobNotFoundException exception) {
            removeStaleIndexes(
                    jobId,
                    submission);
            return null;
        }

        JobExecutionMetadata metadata =
                getMetadata(jobId);

        if (metadata == null
                || !submission.getExternalExecutionId()
                .equals(
                        metadata.getExternalExecutionId())
                || !submission.getIdempotencyKey()
                .equals(
                        metadata.getIdempotencyKey())
                || submission.getDefinitionVersion()
                != metadata.getDefinitionVersion()
                || !submission.getConfigDigest()
                .equals(
                        metadata.getConfigDigest())) {

            throw new JobSubmissionConflictException(
                    "The idempotency key or external execution ID was reused with different content");
        }

        return snapshot;
    }

    private void registerSubmission(
            String jobId,
            JobSubmission submission) {

        String existingByIdempotency =
                jobIdsByIdempotencyKey.putIfAbsent(
                        submission.getIdempotencyKey(),
                        jobId);

        if (existingByIdempotency != null) {
            throw new JobSubmissionConflictException(
                    "idempotencyKey already belongs to job "
                            + existingByIdempotency);
        }

        String existingByExternal =
                jobIdsByExternalExecutionId.putIfAbsent(
                        submission.getExternalExecutionId(),
                        jobId);

        if (existingByExternal != null) {
            jobIdsByIdempotencyKey.remove(
                    submission.getIdempotencyKey(),
                    jobId);

            throw new JobSubmissionConflictException(
                    "externalExecutionId already belongs to job "
                            + existingByExternal);
        }
    }

    private void unregisterSubmission(
            String jobId,
            JobSubmission submission) {

        jobIdsByIdempotencyKey.remove(
                submission.getIdempotencyKey(),
                jobId);
        jobIdsByExternalExecutionId.remove(
                submission.getExternalExecutionId(),
                jobId);
    }

    private void removeStaleIndexes(
            String jobId,
            JobSubmission submission) {

        jobIdsByIdempotencyKey.remove(
                submission.getIdempotencyKey(),
                jobId);
        jobIdsByExternalExecutionId.remove(
                submission.getExternalExecutionId(),
                jobId);
    }

    private void executeJob(
            final JobExecutionHandle handle) {

        if (!handle.markRunning()) {
            if (!handle.getStatus().isTerminal()) {
                completeAndArchive(
                        handle,
                        handle.isCancellationRequested()
                                ? ServerJobStatus.CANCELED
                                : ServerJobStatus.FAILED,
                        null,
                        null);
            }
            return;
        }

        try {
            JobResult result =
                    jobExecutor.execute(
                            handle.getDefinition(),
                            new JobExecutionListener() {
                                public void onJobLogCreated(
                                        String runId,
                                        String jobLogFile) {

                                    handle.bindLogIdentity(
                                            runId,
                                            jobLogFile);
                                }

                                public void onJobExecutionCreated(
                                        JobExecution execution) {

                                    handle.bindExecution(
                                            execution);
                                }
                            });

            ServerJobStatus finalStatus =
                    resolveStatus(
                            handle,
                            result);

            Throwable failure =
                    finalStatus
                            == ServerJobStatus.FAILED
                            ? result.getFailure()
                            : null;

            completeAndArchive(
                    handle,
                    finalStatus,
                    result,
                    failure);

        } catch (Throwable failure) {
            boolean canceled =
                    handle.isCancellationRequested()
                            || failure
                            instanceof CancellationException
                            || failure
                            instanceof InterruptedException;

            if (failure instanceof InterruptedException) {
                Thread.currentThread()
                        .interrupt();
            }

            completeAndArchive(
                    handle,
                    canceled
                            ? ServerJobStatus.CANCELED
                            : ServerJobStatus.FAILED,
                    null,
                    canceled
                            ? null
                            : failure);

            if (failure instanceof Error) {
                throw (Error) failure;
            }
        }
    }

    private ServerJobStatus resolveStatus(
            JobExecutionHandle handle,
            JobResult result) {

        if (handle.isCancellationRequested()
                || result.getStatus()
                == JobStatus.CANCELED) {

            return ServerJobStatus.CANCELED;
        }

        if (result.getStatus()
                == JobStatus.SUCCEEDED) {

            return ServerJobStatus.SUCCEEDED;
        }

        return ServerJobStatus.FAILED;
    }

    private void completeAndArchive(
            JobExecutionHandle handle,
            ServerJobStatus status,
            JobResult result,
            Throwable failure) {

        if (!handle.complete(
                status,
                result,
                failure)) {
            return;
        }

        JobSnapshot snapshot =
                JobSnapshotFactory.create(handle);

        repository.save(
                snapshot,
                handle.metadata());

        runningJobs.remove(
                handle.getJobId(),
                handle);
    }

    public JobSnapshot getJob(String jobId) {
        requireJobId(jobId);

        JobExecutionHandle running =
                runningJobs.get(jobId);

        if (running != null) {
            return JobSnapshotFactory.create(
                    running);
        }

        JobSnapshot finished =
                repository.get(jobId);

        if (finished == null) {
            throw new JobNotFoundException(jobId);
        }

        return finished;
    }

    public JobSnapshot getJobByExternalExecutionId(
            String externalExecutionId) {

        requireText(
                externalExecutionId,
                "externalExecutionId");

        String jobId =
                jobIdsByExternalExecutionId.get(
                        externalExecutionId.trim());

        if (jobId == null) {
            throw new JobNotFoundException(
                    externalExecutionId);
        }

        return getJob(jobId);
    }

    public JobExecutionMetadata getMetadata(
            String jobId) {

        requireJobId(jobId);

        JobExecutionHandle running =
                runningJobs.get(jobId);

        if (running != null) {
            return running.metadata();
        }

        return repository.getMetadata(jobId);
    }

    public List<JobSnapshot> listJobs() {
        Map<String, JobSnapshot> snapshots =
                new LinkedHashMap<String, JobSnapshot>();

        for (JobSnapshot snapshot :
                repository.list()) {

            snapshots.put(
                    snapshot.getJobId(),
                    snapshot);
        }

        for (JobExecutionHandle handle :
                runningJobs.values()) {

            JobSnapshot snapshot =
                    JobSnapshotFactory.create(
                            handle);

            snapshots.put(
                    snapshot.getJobId(),
                    snapshot);
        }

        List<JobSnapshot> result =
                new ArrayList<JobSnapshot>(
                        snapshots.values());

        Collections.sort(
                result,
                new Comparator<JobSnapshot>() {
                    public int compare(
                            JobSnapshot left,
                            JobSnapshot right) {

                        return Long.compare(
                                right.getCreateTimeMillis(),
                                left.getCreateTimeMillis());
                    }
                });

        return result;
    }

    public JobSnapshot cancel(String jobId) {
        requireJobId(jobId);

        JobExecutionHandle handle =
                runningJobs.get(jobId);

        if (handle == null) {
            JobSnapshot finished =
                    repository.get(jobId);

            if (finished != null) {
                throw new JobStateConflictException(
                        jobId,
                        finished.getStatus());
            }

            throw new JobNotFoundException(jobId);
        }

        JobExecutionHandle.CancelResult result =
                handle.requestCancel();

        if (result
                == JobExecutionHandle.CancelResult
                .CANCELLED_BEFORE_START) {

            Thread jobThread =
                    jobThreads.remove(jobId);
            if (jobThread != null) {
                jobThread.interrupt();
            }
            admissionSemaphore.release();

            completeAndArchive(
                    handle,
                    ServerJobStatus.CANCELED,
                    null,
                    null);
        }

        return getJob(jobId);
    }

    public int getRunningJobCount() {
        int count = 0;

        for (JobExecutionHandle handle :
                runningJobs.values()) {
            if (handle.getStatus()
                    == ServerJobStatus.RUNNING) {
                count++;
            }
        }

        return count;
    }

    public int getQueuedJobCount() {
        int count = 0;

        for (JobExecutionHandle handle :
                runningJobs.values()) {

            ServerJobStatus status =
                    handle.getStatus();

            if (status == ServerJobStatus.CREATED
                    || status == ServerJobStatus.SUBMITTED
                    || status == ServerJobStatus.QUEUED) {
                count++;
            }
        }

        return count;
    }

    public int getActiveJobCount() {
        return runningJobs.size();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void close() {
        if (!closed.compareAndSet(
                false,
                true)) {
            return;
        }

        for (JobExecutionHandle handle :
                runningJobs.values()) {

            try {
                JobExecutionHandle.CancelResult result =
                        handle.requestCancel();

                if (result
                        == JobExecutionHandle.CancelResult
                        .CANCELLED_BEFORE_START) {

                    Thread jobThread =
                            jobThreads.remove(
                                    handle.getJobId());
                    if (jobThread != null) {
                        jobThread.interrupt();
                    }
                    admissionSemaphore.release();

                    completeAndArchive(
                            handle,
                            ServerJobStatus.CANCELED,
                            null,
                            null);
                }
            } catch (RuntimeException ignored) {
                // 关闭阶段继续处理其他 Job。
            }
        }

        /*
         * 中断所有仍在运行的 Job 线程，
         * 等待其在超时时间内响应中断。
         */
        for (Thread thread : jobThreads.values()) {
            thread.interrupt();
        }

        long deadline =
                System.currentTimeMillis()
                        + shutdownTimeoutMillis;

        for (Thread thread : jobThreads.values()) {
            long remaining =
                    deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            try {
                thread.join(remaining);
            } catch (InterruptedException exception) {
                Thread.currentThread()
                        .interrupt();
                break;
            }
        }

        /*
         * 关闭超时后仍未返回终态的作业，其真实结果已经无法确认，
         * 必须标记为 LOST，而不是伪装成 CANCELED。
         */
        for (JobExecutionHandle handle :
                runningJobs.values()) {

            completeAndArchive(
                    handle,
                    ServerJobStatus.LOST,
                    null,
                    null);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Job manager is closed");
        }
    }

    private static void requireJobId(
            String jobId) {

        requireText(jobId, "jobId");
    }

    private static void requireText(
            String value,
            String name) {

        if (value == null
                || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + " must not be blank");
        }
    }


}
