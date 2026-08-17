package com.link.up.connector.doris.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;

/**
 * FluxDataType → Doris 数据类型映射。
 *
 * <p>用于自动建表时生成 DDL 字段描述。
 */
public final class DorisSchemaMapper {

    private DorisSchemaMapper() {
    }

    /**
     * 将 FluxDataType 映射为 Doris 列类型字符串。
     */
    public static String toDorisType(FluxDataType<?> dataType) {
        SqlType sqlType = dataType.getSqlType();

        switch (sqlType) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                if (dataType instanceof DecimalType) {
                    DecimalType dt = (DecimalType) dataType;
                    return "DECIMAL(" + dt.getPrecision() + ", " + dt.getScale() + ")";
                }
                return "DECIMAL(38, 18)";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "DATETIME";
            case TIME:
                return "VARCHAR(20)";
            case STRING:
                return "STRING";
            case BYTES:
                return "STRING";
            case ARRAY:
                return "ARRAY<VARCHAR>";
            case MAP:
                return "MAP<VARCHAR, VARCHAR>";
            default:
                return "STRING";
        }
    }

    /**
     * 将 TableSchema 生成 Doris 建表字段描述。
     */
    public static String toDorisFieldDescription(TableSchema schema) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Column col = schema.getColumn(i);
            sb.append('`').append(col.getName()).append("` ")
                    .append(toDorisType(col.getDataType()));
            if (!col.isNullable()) {
                sb.append(" NOT NULL");
            }
            if (col.getDefaultValue() != null) {
                sb.append(" DEFAULT '").append(col.getDefaultValue()).append("'");
            }
        }
        return sb.toString();
    }

    /**
     * 生成 Doris 默认建表 DDL。
     */
    public static String generateCreateTableDdl(
            String database,
            String tableName,
            TableSchema schema,
            String comment) {

        String fields = toDorisFieldDescription(schema);

        String primaryKeys = extractPrimaryKeys(schema);

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS `")
                .append(database).append("`.`").append(tableName).append("` (")
                .append(fields).append(")");

        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            ddl.append(" ENGINE=OLAP UNIQUE KEY (").append(primaryKeys).append(")");
        } else {
            ddl.append(" ENGINE=OLAP DUPLICATE KEY (`")
                    .append(schema.getColumn(0).getName()).append("`)");
        }

        if (comment != null && !comment.isEmpty()) {
            ddl.append(" COMMENT '").append(comment).append("'");
        }

        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            ddl.append(" DISTRIBUTED BY HASH (").append(primaryKeys).append(")");
        } else {
            ddl.append(" DISTRIBUTED BY HASH (`")
                    .append(schema.getColumn(0).getName()).append("`)");
        }

        ddl.append(" PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\")");

        return ddl.toString();
    }

    private static String extractPrimaryKeys(TableSchema schema) {
        if (schema.getPrimaryKey() == null
                || schema.getPrimaryKey().getColumnNames() == null
                || schema.getPrimaryKey().getColumnNames().isEmpty()) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String pk : schema.getPrimaryKey().getColumnNames()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append('`').append(pk).append('`');
        }
        return sb.toString();
    }
}
