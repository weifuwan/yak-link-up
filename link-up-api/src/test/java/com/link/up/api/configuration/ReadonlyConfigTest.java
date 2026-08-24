package com.link.up.api.configuration;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReadonlyConfigTest {

    @Test
    public void shouldTakeDeepImmutableSnapshotOfSourceMap() {
        List<String> tags =
                new ArrayList<String>(
                        Arrays.asList("a", "b"));

        Map<String, Object> jdbc =
                new LinkedHashMap<String, Object>();
        jdbc.put("url", "jdbc:test");
        jdbc.put("tags", tags);

        Map<String, Object> source =
                new LinkedHashMap<String, Object>();
        source.put("jdbc", jdbc);

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(source);

        jdbc.put("url", "jdbc:changed");
        tags.add("c");

        Option<String> url =
                Options.key("jdbc.url")
                        .stringType()
                        .noDefaultValue();

        Option<List<String>> optionTags =
                Options.key("jdbc.tags")
                        .listType()
                        .noDefaultValue();

        assertEquals("jdbc:test", config.get(url));
        assertEquals(
                Arrays.asList("a", "b"),
                config.get(optionTags));
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("unchecked")
    public void shouldNotExposeMutableNestedMaps() {
        Map<String, Object> jdbc =
                new LinkedHashMap<String, Object>();
        jdbc.put("url", "jdbc:test");

        Map<String, Object> source =
                new LinkedHashMap<String, Object>();
        source.put("jdbc", jdbc);

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(source);

        Map<String, Object> nested =
                (Map<String, Object>)
                        config.getSourceMap().get("jdbc");

        nested.put("url", "jdbc:changed");
    }
}
