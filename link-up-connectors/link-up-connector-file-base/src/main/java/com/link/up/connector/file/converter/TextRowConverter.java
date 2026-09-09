package com.link.up.connector.file.converter;

import com.link.up.api.table.type.FluxRow;

/** Maps one whole text line to the single string column. */
public final class TextRowConverter implements FileRowConverter {

    @Override
    public FluxRow convert(String line, String rowContext) {
        return FluxRow.of(line);
    }
}
