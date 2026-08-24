package com.link.up.connector.http.source;

import com.link.up.connector.http.config.HttpSourceConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpPaginationStateTest {

    @Test
    public void shouldAllowExactlyOneRequestWithoutPagination() {
        HttpSourceConfig config =
                new HttpSourceConfig.Builder()
                        .url("http://localhost/items")
                        .build();

        HttpPaginationState state =
                new HttpPaginationState(config);

        assertFalse(state.isExhausted());
        assertTrue(
                state.currentRequest()
                        .getParams()
                        .isEmpty());

        state.advance("{}", 1);
        assertTrue(state.isExhausted());
    }

    @Test
    public void shouldAdvancePageNumberWithoutChangingRequestContract() {
        HttpSourceConfig config =
                new HttpSourceConfig.Builder()
                        .url("http://localhost/items")
                        .pageField("page")
                        .pageBatchSize(2)
                        .totalPageSize(2)
                        .startPageNumber(1)
                        .build();

        HttpPaginationState state =
                new HttpPaginationState(config);

        HttpPageRequest first =
                state.currentRequest();

        assertEquals(
                Collections.singletonMap("page", "1"),
                first.getParams());
        assertFalse(state.isExhausted());

        state.advance("{}", 2);

        HttpPageRequest second =
                state.currentRequest();

        assertEquals(
                Collections.singletonMap("page", "2"),
                second.getParams());

        state.advance("{}", 2);
        assertTrue(state.isExhausted());
    }

    @Test
    public void shouldReplacePagePlaceholderInHeadersAndBody() {
        HttpSourceConfig config =
                new HttpSourceConfig.Builder()
                        .url("http://localhost/items")
                        .headers(
                                Collections.singletonMap(
                                        "X-Page",
                                        "${page}"))
                        .body("{\"page\":\"${page}\"}")
                        .pageField("page")
                        .usePlaceholderReplacement(true)
                        .totalPageSize(1)
                        .build();

        HttpPageRequest request =
                new HttpPaginationState(config)
                        .currentRequest();

        assertEquals(
                "1",
                request.getHeaders().get("X-Page"));
        assertEquals(
                "{\"page\":\"1\"}",
                request.getBody());
    }
}
