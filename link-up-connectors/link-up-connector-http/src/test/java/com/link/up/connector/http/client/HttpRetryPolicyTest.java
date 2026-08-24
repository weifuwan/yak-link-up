package com.link.up.connector.http.client;

import com.link.up.connector.http.config.HttpMethod;
import com.link.up.connector.http.config.HttpSourceConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpRetryPolicyTest {

    @Test
    public void shouldTreatGetAsRetryableAndRespectConfiguredStatusCodes() {
        HttpSourceConfig config =
                new HttpSourceConfig.Builder()
                        .url("http://localhost/items")
                        .method(HttpMethod.GET)
                        .retry(2)
                        .retryableStatusCodes(
                                Collections.singleton(503))
                        .retryJitterMs(0)
                        .retryBackoffMultiplierMs(100)
                        .retryBackoffMaxMs(1000)
                        .build();

        HttpRetryPolicy policy =
                new HttpRetryPolicy(config);

        assertEquals(3, policy.maxAttempts());
        assertTrue(policy.isMethodRetryable());
        assertTrue(policy.isStatusRetryable(503));
        assertFalse(policy.isStatusRetryable(400));
        assertEquals(100L, policy.backoffMillis(1));
        assertEquals(200L, policy.backoffMillis(2));
    }

    @Test
    public void shouldNotRetryPostByDefault() {
        HttpSourceConfig config =
                new HttpSourceConfig.Builder()
                        .url("http://localhost/items")
                        .method(HttpMethod.POST)
                        .build();

        assertFalse(
                new HttpRetryPolicy(config)
                        .isMethodRetryable());
    }
}
