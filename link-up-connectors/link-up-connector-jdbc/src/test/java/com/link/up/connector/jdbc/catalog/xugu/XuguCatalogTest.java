package com.link.up.connector.jdbc.catalog.xugu;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class XuguCatalogTest {

    @Test
    public void escapesMetadataWildcardCharactersForExactLookup() {
        assertEquals(
                "APP\\_DATA\\%2026",
                XuguCatalog.escapeMetadataPattern(
                        "APP_DATA%2026",
                        "\\"));
    }

    @Test
    public void escapesExistingSearchEscapeBeforeWildcards() {
        assertEquals(
                "APP\\\\ARCHIVE\\_2026",
                XuguCatalog.escapeMetadataPattern(
                        "APP\\ARCHIVE_2026",
                        "\\"));
    }

    @Test
    public void leavesExactValueUntouchedWhenDriverHasNoEscape() {
        assertEquals(
                "APP_DATA",
                XuguCatalog.escapeMetadataPattern("APP_DATA", null));
    }
}
