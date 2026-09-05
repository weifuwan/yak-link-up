package com.link.up.connector.jdbc.core.dialect.sqlserver;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class SqlServerTypeMapperTest {

    private final SqlServerTypeMapper mapper = new SqlServerTypeMapper();

    @Test
    public void mapsSqlServerNativeTypesWithoutLosingMeaning() {
        assertEquals(BasicType.SHORT_TYPE,
                mapper.mapNativeType("tinyint", Types.TINYINT, 3, 0));
        assertEquals(BasicType.BYTES_TYPE,
                mapper.mapNativeType("timestamp", Types.BINARY, 8, 0));
        assertEquals(BasicType.TIMESTAMP_TZ_TYPE,
                mapper.mapNativeType("datetimeoffset", Types.TIMESTAMP_WITH_TIMEZONE, 34, 7));
        assertEquals(new DecimalType(19, 4),
                mapper.mapNativeType("money", Types.DECIMAL, 19, 4));
    }

    @Test
    public void mapsFluxTypesToWritableSqlServerTypes() {
        assertEquals("NVARCHAR(200)", mapper.toDatabaseType(
                Column.builder("name", BasicType.STRING_TYPE).length(200L).build()));
        assertEquals("NVARCHAR(MAX)", mapper.toDatabaseType(
                Column.builder("body", BasicType.STRING_TYPE).length(5000L).build()));
        assertEquals("VARBINARY(MAX)", mapper.toDatabaseType(
                Column.builder("payload", BasicType.BYTES_TYPE).length(9000L).build()));
        assertEquals("DATETIMEOFFSET(7)", mapper.toDatabaseType(
                Column.builder("created_at", BasicType.TIMESTAMP_TZ_TYPE)
                        .precision(7).build()));
    }

    @Test
    public void rowversionIsNotPreservedAsWritableTimestamp() {
        assertEquals("VARBINARY(8)", mapper.toDatabaseType(
                Column.builder("version", BasicType.BYTES_TYPE)
                        .length(8L).sourceType("timestamp").build(), true));
    }
}
