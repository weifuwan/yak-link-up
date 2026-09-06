package com.link.up.connector.jdbc.core.dialect.hana;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class HanaTypeMapperTest {

    private final HanaTypeMapper mapper = new HanaTypeMapper();

    @Test
    public void mapsCommonHanaTypesForOfflineSource() {
        assertSame(BasicType.BOOLEAN_TYPE,
                mapper.mapType(Types.BOOLEAN, "BOOLEAN", 1, 0));
        assertSame(BasicType.SHORT_TYPE,
                mapper.mapType(Types.TINYINT, "TINYINT", 3, 0));
        assertSame(BasicType.SHORT_TYPE,
                mapper.mapType(Types.SMALLINT, "SMALLINT", 5, 0));
        assertSame(BasicType.INT_TYPE,
                mapper.mapType(Types.INTEGER, "INTEGER", 10, 0));
        assertSame(BasicType.LONG_TYPE,
                mapper.mapType(Types.BIGINT, "BIGINT", 19, 0));
        assertSame(BasicType.FLOAT_TYPE,
                mapper.mapType(Types.REAL, "REAL", 7, 0));
        assertSame(BasicType.DOUBLE_TYPE,
                mapper.mapType(Types.DOUBLE, "DOUBLE", 15, 0));
        assertSame(BasicType.STRING_TYPE,
                mapper.mapType(Types.NVARCHAR, "NVARCHAR", 500, 0));
        assertSame(BasicType.STRING_TYPE,
                mapper.mapType(Types.CLOB, "NCLOB", 0, 0));
        assertSame(BasicType.BYTES_TYPE,
                mapper.mapType(Types.BLOB, "BLOB", 0, 0));
        assertSame(BasicType.DATE_TYPE,
                mapper.mapType(Types.DATE, "DATE", 10, 0));
        assertSame(BasicType.TIME_TYPE,
                mapper.mapType(Types.TIME, "TIME", 8, 0));
        assertSame(BasicType.TIMESTAMP_TYPE,
                mapper.mapType(Types.TIMESTAMP, "SECONDDATE", 19, 0));
        assertSame(BasicType.TIMESTAMP_TYPE,
                mapper.mapType(Types.TIMESTAMP, "TIMESTAMP", 27, 7));
    }

    @Test
    public void mapsDecimalWithinHanaPrecisionLimit() {
        DecimalType decimal = (DecimalType)
                mapper.mapType(Types.DECIMAL, "DECIMAL", 20, 4);
        assertEquals(20, decimal.getPrecision());
        assertEquals(4, decimal.getScale());

        DecimalType smallDecimal = (DecimalType)
                mapper.mapType(Types.DECIMAL, "SMALLDECIMAL", 16, 3);
        assertEquals(16, smallDecimal.getPrecision());
        assertEquals(3, smallDecimal.getScale());
    }

    @Test
    public void mapsFluxTypesToSafeHanaSinkTypes() {
        assertEquals("SMALLINT",
                mapper.toDatabaseType(
                        Column.builder("TINY", BasicType.BYTE_TYPE).build()));
        assertEquals("NVARCHAR(128)",
                mapper.toDatabaseType(
                        Column.builder("NAME", BasicType.STRING_TYPE)
                                .length(128L)
                                .build()));
        assertEquals("NCLOB",
                mapper.toDatabaseType(
                        Column.builder("BODY", BasicType.STRING_TYPE).build()));
        assertEquals("VARBINARY(1024)",
                mapper.toDatabaseType(
                        Column.builder("PAYLOAD", BasicType.BYTES_TYPE)
                                .length(1024L)
                                .build()));
        assertEquals("BLOB",
                mapper.toDatabaseType(
                        Column.builder("PAYLOAD", BasicType.BYTES_TYPE).build()));
        assertEquals("DECIMAL(20,4)",
                mapper.toDatabaseType(
                        Column.builder("AMOUNT", new DecimalType(20, 4))
                                .precision(20)
                                .scale(4)
                                .build()));
        assertEquals("TIMESTAMP",
                mapper.toDatabaseType(
                        Column.builder("CREATED_AT", BasicType.TIMESTAMP_TYPE).build()));
    }

    @Test
    public void preservesNativeHanaTypeWhenCopyingHanaToHana() {
        Column tinyint = Column.builder("LEVEL", BasicType.SHORT_TYPE)
                .sourceType("TINYINT")
                .build();
        assertEquals("TINYINT", mapper.toDatabaseType(tinyint, true));

        Column secondDate = Column.builder("CREATED_AT", BasicType.TIMESTAMP_TYPE)
                .sourceType("SECONDDATE")
                .build();
        assertEquals("SECONDDATE", mapper.toDatabaseType(secondDate, true));
    }

    @Test
    public void rejectsSpatialArrayAndTimezoneTypesInsteadOfCoercingThem() {
        assertUnsupported("ST_GEOMETRY");
        assertUnsupported("ST_POINT");
        assertUnsupported("INTEGER ARRAY");

        try {
            mapper.toDatabaseType(
                    Column.builder("TS", BasicType.TIMESTAMP_TZ_TYPE).build());
            fail("Expected TIMESTAMP_TZ to be rejected for HANA sink");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private void assertUnsupported(String typeName) {
        try {
            mapper.mapType(Types.OTHER, typeName, 0, 0);
            fail("Expected unsupported HANA type: " + typeName);
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
