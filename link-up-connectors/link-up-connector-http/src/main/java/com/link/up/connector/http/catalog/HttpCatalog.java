package com.link.up.connector.http.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.connector.http.client.HttpSourceClient;
import com.link.up.connector.http.config.HttpFormat;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.schema.HttpSchemaParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP Catalog 实现。
 *
 * <p>HTTP 连接器没有传统数据库的库/表概念，
 * Catalog 将整个 HTTP 端点建模为一张虚拟表。
 *
 * <p>Schema 发现支持两种模式：
 * <ol>
 *   <li><b>配置驱动</b>：用户配置了 {@code schema.fields} 时，
 *       直接通过 {@link HttpSchemaParser} 解析</li>
 *   <li><b>探测推断</b>：未配置 {@code schema.fields} 时，
 *       向端点发送一次探测请求，从 JSON 响应中推断字段类型</li>
 * </ol>
 *
 * <p>Catalog 为只读模式，不支持写操作。
 */
public final class HttpCatalog implements Catalog {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpCatalog.class);

    private static final String CATALOG_NAME = "http";
    private static final String DEFAULT_DATABASE = "default";
    private static final String DEFAULT_TABLE_NAME = "default";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String catalogName;
    private final HttpCatalogConfig config;

    private volatile boolean opened;

    public HttpCatalog(
            String catalogName,
            HttpCatalogConfig config) {

        if (catalogName == null
                || catalogName.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "catalogName must not be empty");
        }

        this.catalogName = catalogName;
        this.config = Objects.requireNonNull(
                config, "config must not be null");
    }

    public HttpCatalog(HttpCatalogConfig config) {
        this(CATALOG_NAME, config);
    }

    // ── Catalog 生命周期 ──────────────────────────────────

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public void open() throws CatalogException {
        if (opened) {
            return;
        }

        LOG.info("Opening HTTP Catalog, url={}",
                config.getUrl());

        opened = true;
    }

    // ── 数据库操作 ──────────────────────────────────────────

    @Override
    public java.util.Optional<String> getDefaultDatabase()
            throws CatalogException {

        checkOpened();
        return java.util.Optional.of(DEFAULT_DATABASE);
    }

    @Override
    public List<String> listDatabases()
            throws CatalogException {

        checkOpened();

        return Collections.singletonList(
                DEFAULT_DATABASE);
    }

    // ── 表操作 ──────────────────────────────────────────

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName)
            throws CatalogException {

        checkOpened();

        String tableName =
                config.getTableName() != null
                        ? config.getTableName()
                        : DEFAULT_TABLE_NAME;

        TablePath tablePath = TablePath.of(
                DEFAULT_DATABASE,
                tableName);

        return Collections.singletonList(
                tablePath);
    }

    @Override
    public boolean tableExists(
            TablePath tablePath)
            throws CatalogException {

        checkOpened();
        Objects.requireNonNull(
                tablePath,
                "tablePath must not be null");

        String expectedName =
                config.getTableName() != null
                        ? config.getTableName()
                        : DEFAULT_TABLE_NAME;

        return expectedName.equals(
                tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(
            TablePath tablePath)
            throws CatalogException,
            TableNotFoundException {

        checkOpened();
        Objects.requireNonNull(
                tablePath,
                "tablePath must not be null");

        if (!tableExists(tablePath)) {
            throw new TableNotFoundException(
                    catalogName,
                    tablePath);
        }

        LOG.info("Discovering schema for HTTP table: {}",
                tablePath);

        TableSchema schema = discoverSchema();

        return CatalogTable.builder(
                        tablePath, schema)
                .comment("HTTP Source: " + config.getUrl())
                .option("dialect", "http")
                .option("url", config.getUrl())
                .build();
    }

    // ── Schema 发现 ──────────────────────────────────────────

    /**
     * 发现 HTTP 端点的 Schema。
     *
     * <p>优先使用用户配置的 {@code schema.fields}；
     * 未配置时，发送探测请求从 JSON 响应推断。
     */
    private TableSchema discoverSchema() {
        if (config.hasSchemaFields()) {
            LOG.debug("使用配置的 schema.fields 构建表结构");
            return HttpSchemaParser.parse(
                    config.getSchemaFields());
        }

        LOG.debug("schema.fields 未配置，发送探测请求推断 Schema");
        return probeAndInferSchema();
    }

    /**
     * 向 HTTP 端点发送一次探测请求，
     * 从 JSON 响应中推断字段类型。
     */
    private TableSchema probeAndInferSchema() {
        HttpSourceConfig probeConfig =
                config.toSourceConfig();

        try (HttpSourceClient client =
                     new HttpSourceClient(probeConfig)) {

            String responseBody = client.execute(
                    config.getHeaders(),
                    config.getParams(),
                    config.getBody());

            if (responseBody == null
                    || responseBody.isEmpty()) {

                throw new CatalogException(
                        "HTTP 探测请求返回空响应，"
                                + "无法推断 Schema，url="
                                + config.getUrl());
            }

            if (config.getFormat() != HttpFormat.JSON) {
                throw new CatalogException(
                        "TEXT 格式不支持自动 Schema 推断，"
                                + "请配置 schema.fields，url="
                                + config.getUrl());
            }

            return inferSchemaFromJson(responseBody);

        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    "HTTP Schema 探测失败，url="
                            + config.getUrl()
                            + ": " + e.getMessage(),
                    e);
        }
    }

    /**
     * 从 JSON 响应中推断 TableSchema。
     *
     * <p>支持 content_field 提取数据数组，
     * 然后从第一个 JSON 对象推断字段类型。
     */
    private TableSchema inferSchemaFromJson(
            String responseBody) throws Exception {

        JsonNode root = MAPPER.readTree(responseBody);

        // 如果配置了 content_field，先提取数据节点
        JsonNode dataNode = root;
        String contentField = config.getContentField();

        if (contentField != null
                && !contentField.isEmpty()) {

            dataNode = navigateJsonPath(
                    root, contentField);

            if (dataNode == null
                    || dataNode.isMissingNode()) {

                throw new CatalogException(
                        "content_field='"
                                + contentField
                                + "' 在响应中未找到数据，"
                                + "无法推断 Schema");
            }
        }

        // 找到第一个 JSON 对象用于推断
        JsonNode sampleObject = findFirstObject(dataNode);

        if (sampleObject == null
                || !sampleObject.isObject()) {

            throw new CatalogException(
                    "HTTP 响应中未找到 JSON 对象，"
                            + "无法推断 Schema。"
                            + "请配置 schema.fields 定义表结构。");
        }

        return buildSchemaFromJsonNode(sampleObject);
    }

    /**
     * 简单的 JsonPath 导航，
     * 支持 {@code $.data.list} 形式。
     */
    private static JsonNode navigateJsonPath(
            JsonNode root,
            String jsonPath) {

        if (jsonPath == null
                || jsonPath.isEmpty()) {

            return root;
        }

        String path = jsonPath.trim();

        // 去掉开头的 $.
        if (path.startsWith("$.")) {
            path = path.substring(2);
        } else if (path.startsWith("$")) {
            path = path.substring(1);
        }

        if (path.isEmpty()) {
            return root;
        }

        JsonNode current = root;

        for (String segment : path.split("\\.")) {
            if (current == null
                    || current.isMissingNode()) {

                return null;
            }

            // 处理数组索引 [*]
            String seg = segment.replace("[*]", "");

            if (!seg.isEmpty()) {
                current = current.get(seg);
            }
        }

        return current;
    }

    /**
     * 从 JsonNode 中找到第一个 JSON 对象。
     */
    private static JsonNode findFirstObject(JsonNode node) {
        if (node == null) {
            return null;
        }

        if (node.isObject()) {
            return node;
        }

        if (node.isArray()) {
            for (JsonNode element : node) {
                if (element.isObject()) {
                    return element;
                }
            }
        }

        return null;
    }

    /**
     * 从 JSON 对象的字段推断 TableSchema。
     */
    private static TableSchema buildSchemaFromJsonNode(
            JsonNode object) {

        TableSchema.Builder builder =
                TableSchema.builder();

        Iterator<Map.Entry<String, JsonNode>> fields =
                object.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field =
                    fields.next();

            String fieldName = field.getKey();
            JsonNode valueNode = field.getValue();

            FluxDataType<?> dataType =
                    inferFluxType(valueNode);

            String sourceType =
                    jsonTypeDescription(valueNode);

            builder.column(
                    Column.builder(
                                    fieldName, dataType)
                            .nullable(true)
                            .sourceType(sourceType)
                            .build());
        }

        TableSchema schema = builder.build();

        if (schema.getColumns().isEmpty()) {
            throw new CatalogException(
                    "JSON 对象无字段，无法推断 Schema");
        }

        return schema;
    }

    /**
     * 根据 JSON 值推断 FluxDataType。
     */
    private static FluxDataType<?> inferFluxType(
            JsonNode node) {

        if (node == null
                || node.isNull()
                || node.isMissingNode()) {

            // null 值默认为 STRING
            return BasicType.STRING_TYPE;
        }

        if (node.isBoolean()) {
            return BasicType.BOOLEAN_TYPE;
        }

        if (node.isInt()) {
            return BasicType.INT_TYPE;
        }

        if (node.isLong()) {
            return BasicType.LONG_TYPE;
        }

        if (node.isFloat()
                || node.isDouble()) {

            return BasicType.DOUBLE_TYPE;
        }

        if (node.isBigDecimal()) {
            return BasicType.DOUBLE_TYPE;
        }

        if (node.isTextual()) {
            String text = node.asText();

            // 尝试识别常见日期/时间格式
            if (isTimestampLike(text)) {
                return BasicType.TIMESTAMP_TYPE;
            }

            if (isDateLike(text)) {
                return BasicType.DATE_TYPE;
            }

            return BasicType.STRING_TYPE;
        }

        // 对象、数组等复杂类型统一映射为 STRING
        return BasicType.STRING_TYPE;
    }

    /**
     * 生成 JSON 值的类型描述字符串，
     * 存入 Column.sourceType。
     */
    private static String jsonTypeDescription(
            JsonNode node) {

        if (node == null
                || node.isNull()
                || node.isMissingNode()) {

            return "null";
        }

        if (node.isBoolean()) return "json:boolean";
        if (node.isInt()) return "json:int";
        if (node.isLong()) return "json:long";
        if (node.isFloat()
                || node.isDouble()) {
            return "json:double";
        }
        if (node.isBigDecimal()) {
            return "json:decimal";
        }
        if (node.isTextual()) return "json:string";
        if (node.isArray()) return "json:array";
        if (node.isObject()) return "json:object";

        return "json:unknown";
    }

    /**
     * 简单判断字符串是否像时间戳。
     *
     * <p>支持 {@code yyyy-MM-dd HH:mm:ss} 和
     * {@code yyyy-MM-ddTHH:mm:ss} 格式。
     */
    private static boolean isTimestampLike(
            String value) {

        if (value == null
                || value.length() < 19) {

            return false;
        }

        // yyyy-MM-dd HH:mm:ss 或 yyyy-MM-ddTHH:mm:ss
        return value.matches(
                "\\d{4}-\\d{2}-\\d{2}[T ]"
                        + "\\d{2}:\\d{2}:\\d{2}.*");
    }

    /**
     * 简单判断字符串是否像日期。
     *
     * <p>支持 {@code yyyy-MM-dd} 格式。
     */
    private static boolean isDateLike(
            String value) {

        if (value == null
                || value.length() != 10) {

            return false;
        }

        return value.matches(
                "\\d{4}-\\d{2}-\\d{2}");
    }

    // ── 内部工具 ──────────────────────────────────────────

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException(
                    "HTTP Catalog has not been opened");
        }
    }

    @Override
    public void close() throws CatalogException {
        if (!opened) {
            return;
        }

        LOG.info("Closing HTTP Catalog");
        opened = false;
    }
}
