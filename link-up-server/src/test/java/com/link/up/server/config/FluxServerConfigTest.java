package com.link.up.server.config;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class FluxServerConfigTest {

    @Test
    public void shouldParseWorkerStateDirectory() {
        Path expected = Paths.get("target", "phase7-state")
                .toAbsolutePath()
                .normalize();

        FluxServerConfig config = FluxServerConfig.fromArgs(
                new String[] {
                        "--state-dir",
                        expected.toString()
                });

        assertEquals(expected, config.getStateDirectory());
    }
}
