package com.link.up.connector.http.source;

import com.link.up.api.source.SourceSplitEnumerator;

import java.util.Collections;
import java.util.List;

/**
 * Enumerates the bounded HTTP Source work for one planning attempt.
 *
 * <p>HTTP pagination remains reader-owned, so the bounded source exposes one
 * logical split and the reader advances pages inside that split.</p>
 */
public final class HttpSourceSplitEnumerator
        implements SourceSplitEnumerator<HttpSourceSplit> {

    @Override
    public List<HttpSourceSplit> enumerateSplits() {
        return Collections.singletonList(
                new HttpSourceSplit());
    }
}
