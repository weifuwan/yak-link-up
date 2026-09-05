package com.link.up.connector.jdbc.core.dialect.yashandb;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class YashanDbTypeMapperTest {

    private final YashanDbTypeMapper mapper = new YashanDbTypeMapper();

    @Test
    public void mapsRepresentableNumberExactly() {
        assertEquals(
                new DecimalType(38, 10),
                mapper.mapType(Types.NUMERIC, "NUMBER", 38, 10));
        assertEquals(
                "NUMBER(38,10)",
                mapper.toDatabaseType(
                        Column.builder("AMOUNT", new DecimalType(38, 10)).build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetNumberPrecisionAboveYashanLimit() {
        mapper.toDatabaseType(
                Column.builder("AMOUNT", new DecimalType(39, 10)).build());
    }

    @Test
    public void unconstrainedAndNegativeScaleNumbersStayExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMBER", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMBER", 20, -2));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMBER", 5, 10));
    }

    @Test
    public void bitWidthControlsBooleanVersusBytes() {
        assertEquals(
                BasicType.BOOLEAN_TYPE,
                mapper.mapType(Types.BIT, "BIT", 1, 0));
        assertEquals(
                BasicType.BYTES_TYPE,
                mapper.mapType(Types.BIT, "BIT", 8, 0));
    }

    @Test
    public void mapsNativeDateToTimestampAndWideTimeToText() {
        assertEquals(
                BasicType.TIMESTAMP_TYPE,
                mapper.mapType(Types.DATE, "DATE", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.TIME, "TIME", 0, 6));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(
                        Types.TIME_WITH_TIMEZONE,
                        "TIME(6) WITH TIME ZONE",
                        0,
                        6));
    }

    @Test
    public void preservesTimestampTimezoneSemantics() {
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP(6) WITH TIME ZONE",
                        0,
                        6));
        assertEquals(
                BasicType.TIMESTAMP_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP,
                        "TIMESTAMP(6) WITH LOCAL TIME ZONE",
                        0,
                        6));
    }

    @Test
    public void widensUnsignedCompatibilityIntegers() {
        assertEquals(
                BasicType.SHORT_TYPE,
                mapper.mapType(Types.OTHER, "TINYINT UNSIGNED", 3, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.OTHER, "SMALLINT UNSIGNED", 5, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.OTHER, "INT UNSIGNED", 10, 0));
        assertEquals(
                new DecimalType(20, 0),
                mapper.mapType(Types.OTHER, "BIGINT UNSIGNED", 20, 0));
    }

    @Test
    public void boundedStringsAndBytesUseInlineTypes() {
        assertEquals(
                "VARCHAR(255 CHAR)",
                mapper.toDatabaseType(
                        Column.builder("NAME", BasicType.STRING_TYPE)
                                .length(255L)
                                .build()));
        assertEquals(
                "RAW(1024)",
                mapper.toDatabaseType(
                        Column.builder("PAYLOAD", BasicType.BYTES_TYPE)
                                .length(1024L)
                                .build()));
    }

    @Test
    public void largeStringsAndBytesFallBackToLobs() {
        assertEquals(
                "CLOB",
                mapper.toDatabaseType(
                        Column.builder("BODY", BasicType.STRING_TYPE)
                                .length(20_000L)
                                .build()));
        assertEquals(
                "BLOB",
                mapper.toDatabaseType(
                        Column.builder("PAYLOAD", BasicType.BYTES_TYPE)
                                .length(9_000L)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsGenericTimestampPrecisionAboveStoredMicroseconds() {
        mapper.toDatabaseType(
                Column.builder("TS", BasicType.TIMESTAMP_TYPE)
                        .precision(7)
                        .build());
    }

    @Test
    public void preservesNativeYashanTypeForSameDialectDdl() {
        Column column = Column.builder("DOC", BasicType.STRING_TYPE)
                .sourceType("JSON")
                .build();
        assertEquals("JSON", mapper.toDatabaseType(column, true));
    }
}
