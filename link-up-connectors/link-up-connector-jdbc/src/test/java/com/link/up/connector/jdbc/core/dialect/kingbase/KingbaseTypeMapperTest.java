package com.link.up.connector.jdbc.core.dialect.kingbase;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class KingbaseTypeMapperTest {

    private final KingbaseTypeMapper mapper = new KingbaseTypeMapper();

    @Test
    public void supportsNumericPrecisionUpToOneThousand() {
        Column column = Column.builder("amount", new DecimalType(1000, 20)).build();
        assertEquals("NUMERIC(1000,20)", mapper.toDatabaseType(column));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTargetNumericPrecisionAboveKingbaseLimit() {
        mapper.toDatabaseType(
                Column.builder("amount", new DecimalType(1001, 20)).build());
    }

    @Test
    public void unconstrainedAndNegativeScaleNumericUseExactText() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMERIC", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.NUMERIC, "NUMBER", 20, -2));
    }

    @Test
    public void mapsKingbaseIntegerCompatibilityTypesWithoutOverflow() {
        assertEquals(
                BasicType.BYTE_TYPE,
                mapper.mapType(Types.TINYINT, "TINYINT", 3, 0));
        assertEquals(
                BasicType.BYTE_TYPE,
                mapper.mapType(Types.OTHER, "INT1", 3, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "MEDIUMINT", 7, 0));
        assertEquals(
                BasicType.INT_TYPE,
                mapper.mapType(Types.OTHER, "INT3", 7, 0));
    }

    @Test
    public void mapsUnsignedIntegersToWiderLosslessTypes() {
        assertEquals(
                BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "INT UNSIGNED", 10, 0));
        assertEquals(
                new DecimalType(20, 0),
                mapper.mapType(Types.NUMERIC, "BIGINT UNSIGNED", 20, 0));
    }

    @Test
    public void bitAndMoneyAreNotCollapsedToBooleanOrFixedDecimal() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.BIT, "BIT", 8, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "BIT VARYING", 64, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.OTHER, "MONEY", 0, 0));
    }

    @Test
    public void timezoneTypesKeepTheirSemanticInformation() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(
                        Types.TIME_WITH_TIMEZONE,
                        "TIME(6) WITH TIME ZONE",
                        0,
                        6));
        assertEquals(
                BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapType(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP(6) WITH TIME ZONE",
                        0,
                        6));
    }

    @Test
    public void mapsKingbaseLobAndCompatibilityTypes() {
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.CLOB, "CLOB", 0, 0));
        assertEquals(
                BasicType.BYTES_TYPE,
                mapper.mapType(Types.BLOB, "BLOB", 0, 0));
        assertEquals(
                BasicType.STRING_TYPE,
                mapper.mapType(Types.VARCHAR, "VARCHAR2", 255, 0));
    }

    @Test
    public void usesPortableVarcharAndTextForCrossDatabaseStrings() {
        assertEquals(
                "VARCHAR(255)",
                mapper.toDatabaseType(
                        Column.builder("name", BasicType.STRING_TYPE)
                                .length(255L)
                                .build()));
        assertEquals(
                "TEXT",
                mapper.toDatabaseType(
                        Column.builder("payload", BasicType.STRING_TYPE)
                                .length(100_000L)
                                .build()));
    }

    @Test
    public void preservesNativeTypesForKingbaseToKingbaseDdl() {
        Column column = Column.builder("payload", BasicType.STRING_TYPE)
                .sourceType("JSONB")
                .build();
        assertEquals("JSONB", mapper.toDatabaseType(column, true));
    }
}
