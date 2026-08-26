package com.link.up.server.application.port;

/** Lifecycle port for removing event journals outside retained Job history. */
public interface JobEventRetention {

    JobEventRetention EMPTY =
            new JobEventRetention() {
                @Override
                public void delete(String jobId) {
                }

                @Override
                public void retain(Iterable<String> jobIds) {
                }
            };

    void delete(String jobId);

    void retain(Iterable<String> jobIds);
}
