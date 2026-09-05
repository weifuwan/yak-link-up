package com.link.up.connector.jdbc.core.dialect.iris;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class IrisTypeMapperTest {

    private final IrisTypeMapper mapper = new IrisTypeMapper();

    @Test
    public void supportsIrisMaximumExplicitNumericShape() {
        Column column = Column.builder("Amount", new DecimalType(37, 18)).build();
        assertEquals("NUMERIC(37,18)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsPrecisionAboveNineteenPlusScale() {
        mapper.toDatabaseType(
                Column.builder("Amount", new DecimalType(20, 0)).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsScaleAboveEighteen() {
        mapper.toDatabaseType(
                Column.builder("Amount", new DecimalType(38, 19)).build());
    }

    @Test
    public void invalidSourceNumericShapesStayExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 20, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "DECIMAL", 30, 10));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 20, -2));
    }

    @Test
    public void defaultNumericAndNumberSemanticsAreDeterministic() {
        assertEquals(
                new DecimalType(15, 0),
                mapper.mapType(Types.NUMERIC, "NUMERIC", 0, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "NUMBER", 0, 0));
    }

    @Test
    public void mapsIrisIntegerAndCounterTypes() {
        assertEquals(
                BasicType.BYTE_TYPE,
                mapper.mapType(Types.TINYINT, "TINYINT", 3, 0));
        assertEquals(
                BasicType.SHORT_TYPE,
                mapper.mapType(Types.SMALLINT, "SMALLINT", 5, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "MEDIUMINT", 8, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "SERIAL", 19, 0));
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "ROWVERSION", 19, 0));
    }

    @Test
    public void bitIsBooleanAndGuidIsText() {
        assertEquals(
                BasicType.BOOLEAN_TYPE,
                mapper.mapType(Types.BIT, "BIT", 1, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.VARCHAR, "GUID", 36, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.VARCHAR, "UNIQUEIDENTIFIER", 36, 0));
    }

    @Test
    public void mapsLobFamiliesWithoutTruncation() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.LONGVARCHAR, "LONGVARCHAR", Integer.MAX_VALUE, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.CLOB, "CLOB", Integer.MAX_VALUE, 0));
        assertEquals(
                BasicType.BYTES_TYPE,
                mapper.mapType(Types.LONGVARBINARY, "LONGVARBINARY", Integer.MAX_VALUE, 0));
        assertEquals(
                BasicType.BYTES_TYPE,
                mapper.mapType(Types.BLOB, "BLOB", Integer.MAX_VALUE, 0));
    }

    @Test
    public void choosesBoundedAndStreamingTargetTypes() {
        assertEquals(
                "VARCHAR(1024)",
                mapper.toDatabaseType(
                        Column.builder("Name", BasicType.STRING_TYPE)
                                .length(1024L)
                                .build()));
        assertEquals(
                "LONGVARCHAR",
                mapper.toDatabaseType(
                        Column.builder("Payload", BasicType.STRING_TYPE)
                                .length(100_000L)
                                .build()));
        assertEquals(
                "VARBINARY(1024)",
                mapper.toDatabaseType(
                        Column.builder("Body", BasicType.BYTES_TYPE)
                                .length(1024L)
                                .build()));
        assertEquals(
                "LONGVARBINARY",
                mapper.toDatabaseType(
                        Column.builder("Body", BasicType.BYTES_TYPE)
                                .length(100_000L)
                                .build()));
    }

    @Test
    public void mapsTemporalFamiliesAndUsesTimestamp2ForTarget() {
        assertEquals(
                BasicType.TIME_TYPE,
                mapper.mapType(Types.TIME, "TIME", 0, 9));
        assertEquals(
                BasicType.TIMESTAMP_TYPE,
                mapper.mapType(Types.TIMESTAMP, "POSIXTIME", 0, 6));
        assertEquals(
                BasicType.TIMESTAMP_TYPE,
                mapper.mapType(Types.TIMESTAMP, "TIMESTAMP2", 0, 9));
        assertEquals(
                "TIME(9)",
                mapper.toDatabaseType(
                        Column.builder("EventTime", BasicType.TIME_TYPE)
                                .precision(9)
                                .build()));
        assertEquals(
                "TIMESTAMP2",
                mapper.toDatabaseType(
                        Column.builder("CreatedAt", BasicType.TIMESTAMP_TYPE)
                                .precision(9)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimePrecisionAboveNine() {
        mapper.toDatabaseType(
                Column.builder("EventTime", BasicType.TIME_TYPE)
                        .precision(10)
                        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimestampPrecisionAboveNine() {
        mapper.toDatabaseType(
                Column.builder("CreatedAt", BasicType.TIMESTAMP_TYPE)
                        .precision(10)
                        .build());
    }

    @Test
    public void timestampWithOffsetUsesTextOnIrisTarget() {
        assertEquals(
                "VARCHAR(64)",
                mapper.toDatabaseType(
                        Column.builder("OccurredAt", BasicType.TIMESTAMP_TZ_TYPE).build()));
    }

    @Test
    public void sameDialectPreservesSafeTypesButNotReadOnlyRowversionOrVector() {
        Column guid = Column.builder("Id", BasicType.STRING_TYPE)
                .sourceType("GUID")
                .length(36L)
                .build();
        assertEquals("GUID", mapper.toDatabaseType(guid, true));

        Column rowversion = Column.builder("Version", BasicType.LONG_TYPE)
                .sourceType("ROWVERSION")
                .build();
        assertEquals("BIGINT", mapper.toDatabaseType(rowversion, true));

        Column vector = Column.builder("Embedding", BasicType.STRING_TYPE)
                .sourceType("VECTOR(DOUBLE,128)")
                .build();
        assertEquals("LONGVARCHAR", mapper.toDatabaseType(vector, true));
    }
}
