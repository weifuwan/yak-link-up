package com.link.up.connector.jdbc.core.dialect.opengauss;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class OpenGaussTypeMapperTest {

    private final OpenGaussTypeMapper mapper = new OpenGaussTypeMapper();

    @Test
    public void supportsOpenGaussNumericPrecisionUpToOneThousand() {
        Column column = Column.builder("amount", new DecimalType(1000, 20)).build();
        assertEquals("NUMERIC(1000,20)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetNumericPrecisionAboveOpenGaussLimit() {
        mapper.toDatabaseType(
                Column.builder("amount", new DecimalType(1001, 20)).build());
    }

    @Test
    public void unconstrainedNumericIsCarriedAsExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 0, 0));
    }

    @Test
    public void negativeScaleNumericIsCarriedAsExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 20, -2));
    }

    @Test
    public void mapsInt16WithoutLongOverflow() {
        assertEquals(
                new DecimalType(39, 0),
                mapper.mapType(Types.OTHER, "INT16", 39, 0));
    }

    @Test
    public void timeWithTimezoneKeepsOffsetAsText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(
                        Types.TIME_WITH_TIMEZONE,
                        "TIME WITH TIME ZONE",
                        0,
                        6));
    }

    @Test
    public void timestampWithTimezoneUsesOffsetDateTimeType() {
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP WITH TIME ZONE",
                        0,
                        6));
    }

    @Test
    public void usesExplicitCharacterSemanticsForCrossDatabaseStrings() {
        assertEquals(
                "VARCHAR(255 CHAR)",
                mapper.toDatabaseType(
                        Column.builder("name", BasicType.STRING_TYPE)
                                .length(255L)
                                .build()));
    }

    @Test
    public void largeStringsFallBackToText() {
        assertEquals(
                "TEXT",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.STRING_TYPE)
                                .length(3_000_000L)
                                .build()));
    }

    @Test
    public void preservesNativeTypeForOpenGaussToOpenGaussDdl() {
        Column column = Column.builder("payload", BasicType.STRING_TYPE)
                .sourceType("JSONB")
                .build();
        assertEquals("JSONB", mapper.toDatabaseType(column, true));
    }
}
