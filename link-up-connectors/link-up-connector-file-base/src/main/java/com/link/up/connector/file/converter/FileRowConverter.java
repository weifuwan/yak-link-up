package com.link.up.connector.file.converter;

import com.link.up.api.table.type.FluxRow;

/**
 * Converts one raw text line into a {@link FluxRow} over the projected
 * columns.
 *
 * <p>{@code rowContext} carries the file/line identity for error messages so
 * converters stay stateless.
 */
public interface FileRowConverter {

    FluxRow convert(String line, String rowContext);
}
