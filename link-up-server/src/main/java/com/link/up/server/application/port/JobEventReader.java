package com.link.up.server.application.port;

import com.link.up.server.runtime.event.JobEventPage;

/** Query port for one Job's append-only event journal. */
public interface JobEventReader {

    JobEventReader EMPTY =
            new JobEventReader() {
                @Override
                public JobEventPage read(
                        String jobId,
                        long afterSequence,
                        int limit) {
                    return JobEventPage.empty(
                            jobId,
                            afterSequence);
                }
            };

    JobEventPage read(
            String jobId,
            long afterSequence,
            int limit);
}
