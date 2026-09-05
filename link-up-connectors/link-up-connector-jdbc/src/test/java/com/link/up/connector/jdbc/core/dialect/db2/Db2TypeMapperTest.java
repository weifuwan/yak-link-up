package com.link.up.connector.jdbc.core.dialect.db2;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class Db2TypeMapperTest {

    private final Db2TypeMapper mapper = new Db2TypeMapper();

    @Test
    public void preservesSupportedDecimalPrecisionAndScale() {
        Column column = Column.builder("amount", new DecimalType(31, 10))
                .precision(31)
                .scale(10)
                .build();
        assertEquals("DECIMAL(31,10)", mapper.toDatabaseType(column));
    }

    @Test
    public void fallsBackToDecimalTypePrecisionWhenColumnMetadataIsAbsent() {
        Column column = Column.builder("amount", new DecimalType(20, 6)).build();
        assertEquals("DECIMAL(20,6)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDecimalPrecisionAboveDb2LimitInsteadOfTruncatingScale() {
        Column column = Column.builder("amount", new DecimalType(38, 10))
                .precision(38)
                .scale(10)
                .build();
        mapper.toDatabaseType(column);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsIntrinsicDecimalPrecisionAboveDb2Limit() {
        mapper.toDatabaseType(
                Column.builder("amount", new DecimalType(38, 10)).build());
    }

    @Test
    public void mapsDecfloatToExactTextInsteadOfDouble() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "DECFLOAT", 34, 0));
    }

    @Test
    public void preservesDb2DecfloatSourceTypeForDb2ToDb2Ddl() {
        Column column = Column.builder("amount", BasicType.STRING_TYPE)
                .sourceType("DECFLOAT(34)")
                .build();
        assertEquals("DECFLOAT(34)", mapper.toDatabaseType(column, true));
    }

    @Test
    public void mapsLargeStringsAndBytesToLobs() {
        assertEquals(
                "CLOB(50000)",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.STRING_TYPE)
                                .length(50000L)
                                .build()));
        assertEquals(
                "BLOB(50000)",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.BYTES_TYPE)
                                .length(50000L)
                                .build()));
    }

    @Test
    public void capsTimestampPrecisionAtTwelve() {
        assertEquals(
                "TIMESTAMP(12)",
                mapper.toDatabaseType(
                        Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                                .precision(20)
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTimestampWithTimezoneForDb2Luw() {
        mapper.toDatabaseType(
                Column.builder("created_at", BasicType.TIMESTAMP_TZ_TYPE).build());
    }
}
