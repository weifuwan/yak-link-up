package com.link.up.api.connector.schema;

/**
 * Connector 可以向控制面公开的稳定能力标识。
 *
 * <p>这里只描述执行引擎实际支持的能力，不包含具体前端控件和页面布局。
 * Capability 表示“实现可以提供”，是否在某次任务中启用仍由任务配置决定。</p>
 */
public enum ConnectorCapability {
    TABLE_SCHEMA_DISCOVERY,
    MULTI_TABLE,
    CUSTOM_SQL,
    PARTITION_SPLIT,
    UPSERT,
    AUTO_CREATE_TABLE,
    DIRTY_DATA_HANDLING,
    TWO_PHASE_COMMIT
}
