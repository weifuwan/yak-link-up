package com.link.up.connector.jdbc.core.dialect.dameng;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class DamengTypeMapperTest {

    private final DamengTypeMapper mapper = new DamengTypeMapper();

    @Test
    public void preservesMaximumSupportedDecimalPrecision() {
        Column column = Column.builder("AMOUNT", new DecimalType(38, 10)).build();
        assertEquals("DECIMAL(38,10)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDecimalPrecisionAboveDamengLimitInsteadOfTruncating() {
        mapper.toDatabaseType(
                Column.builder("AMOUNT", new DecimalType(39, 10)).build());
    }

    @Test
    public void expandsGenericCharacterLengthToUtf8WorstCaseBytes() {
        assertEquals(
                "VARCHAR2(400)",
                mapper.toDatabaseType(
                        Column.builder("NAME", BasicType.STRING_TYPE)
                                .length(100L)
                                .build()));
    }

    @Test
    public void movesLargeGenericStringsAndBytesToLobs() {
        assertEquals(
                "TEXT",
                mapper.toDatabaseType(
                        Column.builder("CONTENT", BasicType.STRING_TYPE)
                                .length(500L)
                                .build()));
        assertEquals(
                "BLOB",
                mapper.toDatabaseType(
                        Column.builder("PAYLOAD", BasicType.BYTES_TYPE)
                                .length(5000L)
                                .build()));
    }

    @Test
    public void keepsTimeWithTimezoneAsExactTextInsteadOfDroppingOffset() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "TIME WITH TIME ZONE", 0, 6));
    }

    @Test
    public void mapsDatetimeWithTimezoneToTimestampTz() {
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(Types.OTHER, "DATETIME WITH TIME ZONE", 0, 6));
    }

    @Test
    public void mapsDamengDateAsDateNotOracleStyleTimestamp() {
        assertEquals(
                BasicType.DATE_TYPE,
                mapper.mapType(Types.DATE, "DATE", 0, 0));
    }

    @Test
    public void preservesSameDamengSourceTypeWhenSafe() {
        Column column = Column.builder("NAME", BasicType.STRING_TYPE)
                .sourceType("VARCHAR2(120)")
                .length(120L)
                .build();
        assertEquals("VARCHAR2(120)", mapper.toDatabaseType(column, true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimestampPrecisionAboveDamengLimit() {
        mapper.toDatabaseType(
                Column.builder("CREATED_AT", BasicType.TIMESTAMP_TYPE)
                        .precision(7)
                        .build());
    }
}
