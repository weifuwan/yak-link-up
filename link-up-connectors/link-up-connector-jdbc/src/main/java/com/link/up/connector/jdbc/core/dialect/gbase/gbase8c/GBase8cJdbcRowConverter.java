package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

/** GBase 8c bounded Source and existing-table Sink row converter. */
public final class GBase8cJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.GBASE8C;
    }
}
