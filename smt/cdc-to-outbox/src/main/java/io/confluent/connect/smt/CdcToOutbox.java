/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.confluent.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A Single Message Transform that unwraps a CDC change-event envelope (such as the one emitted by
 * Debezium, with {@code before}/{@code after}/{@code op}/{@code ts_ms} fields) and reshapes it into
 * an outbox-pattern message with {@code id}, {@code aggregatetype}, {@code type}, {@code payload}
 * and {@code timestamp} fields.
 *
 * <p>The {@code payload} is the change-event state serialized as a <em>schemaless JSON string</em>
 * (via {@link JsonConverter} with {@code schemas.enable=false}), so the outbox message carries a
 * single static schema regardless of the source table's columns.
 *
 * <p>The transform operates on the record <em>value</em> and supports both schema-based records
 * (Connect {@link Struct}) and schemaless records ({@link Map}). Tombstones (null values) are
 * passed through unchanged.
 *
 * <p>For deletes the {@code after} state is null, so the {@code before} state is used as the
 * payload instead.
 */
public class CdcToOutbox<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Reshape a CDC change-event envelope into an outbox-pattern message "
                    + "(id, aggregatetype, type, payload, timestamp) where payload is a schemaless JSON string.";

    public static final String OUTBOX_SCHEMA_NAME = "io.confluent.connect.smt.Outbox";

    // Standard Debezium-style source metadata block: source.table holds the captured table name.
    private static final String SOURCE_FIELD = "source";
    private static final String TABLE_FIELD = "table";

    // Joins the record key and source table into the outbox id: "<key>.<table>".
    private static final String ID_SEPARATOR = ".";
    // Joins the parts of a composite (multi-column) record key.
    private static final String KEY_SEPARATOR = "_";

    interface ConfigName {
        String ID_FIELD = "id.field";
        String AGGREGATE_TYPE = "aggregate.type";
        String EVENT_TYPE_FIELD = "event.type.field";
        String OP_FIELD = "op.field";
        String BEFORE_FIELD = "before.field";
        String AFTER_FIELD = "after.field";
        String TIMESTAMP_FIELD = "timestamp.field";
        String OUTBOX_TOPIC = "outbox.topic";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.ID_FIELD, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.MEDIUM,
                    "Field within the change-event state to use as the outbox 'id'. If empty, the id "
                            + "is the original record key joined with the captured table name.")
            .define(ConfigName.AGGREGATE_TYPE, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH,
                    "Static value for the outbox 'aggregatetype' field. If empty, the captured table "
                            + "name is used (the envelope 'source.table', falling back to the last "
                            + "dot-separated segment of the record topic).")
            .define(ConfigName.EVENT_TYPE_FIELD, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.MEDIUM,
                    "Field in the envelope (or state) whose value becomes the outbox 'type'. "
                            + "If empty, the change operation ('op') is used.")
            .define(ConfigName.OP_FIELD, ConfigDef.Type.STRING, "op",
                    ConfigDef.Importance.LOW,
                    "Name of the operation field in the CDC envelope.")
            .define(ConfigName.BEFORE_FIELD, ConfigDef.Type.STRING, "before",
                    ConfigDef.Importance.LOW,
                    "Name of the 'before' state field in the CDC envelope.")
            .define(ConfigName.AFTER_FIELD, ConfigDef.Type.STRING, "after",
                    ConfigDef.Importance.LOW,
                    "Name of the 'after' state field in the CDC envelope.")
            .define(ConfigName.TIMESTAMP_FIELD, ConfigDef.Type.STRING, "ts_ms",
                    ConfigDef.Importance.LOW,
                    "Name of the timestamp field in the CDC envelope. Empty to omit.")
            .define(ConfigName.OUTBOX_TOPIC, ConfigDef.Type.STRING, "cdc_outbox",
                    ConfigDef.Importance.HIGH,
                    "Name of the outbox topic.");

    private String idField;
    private String aggregateType;
    private String eventTypeField;
    private String opField;
    private String beforeField;
    private String afterField;
    private String timestampField;
    private String outboxTopic;

    private Schema outboxSchema;
    private JsonConverter jsonConverter;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        idField = config.getString(ConfigName.ID_FIELD);
        aggregateType = config.getString(ConfigName.AGGREGATE_TYPE);
        eventTypeField = config.getString(ConfigName.EVENT_TYPE_FIELD);
        opField = config.getString(ConfigName.OP_FIELD);
        beforeField = config.getString(ConfigName.BEFORE_FIELD);
        afterField = config.getString(ConfigName.AFTER_FIELD);
        timestampField = config.getString(ConfigName.TIMESTAMP_FIELD);
        outboxTopic = config.getString(ConfigName.OUTBOX_TOPIC);
        // Serializes the change-event state to a schemaless JSON string (no schema envelope).
        jsonConverter = new JsonConverter();
        final Map<String, Object> jsonConfig = new HashMap<>();
        jsonConfig.put("schemas.enable", false);
        jsonConfig.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        jsonConverter.configure(jsonConfig);

        outboxSchema = buildOutboxSchema();
    }

    private Schema buildOutboxSchema() {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name(OUTBOX_SCHEMA_NAME)
                .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("aggregatetype", Schema.STRING_SCHEMA)
                .field("type", Schema.OPTIONAL_STRING_SCHEMA)
                .field("payload", Schema.OPTIONAL_STRING_SCHEMA);
        if (!timestampField.isEmpty()) {
            builder.field("timestamp", Schema.OPTIONAL_INT64_SCHEMA);
        }
        return builder.build();
    }

    @Override
    public R apply(R record) {
        // Pass tombstones through unchanged.
        if (record.value() == null) {
            return record;
        }
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        }
        return applyWithSchema(record);
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        final Map<String, Object> envelope = (Map<String, Object>) record.value();

        if (!envelope.containsKey(afterField) && !envelope.containsKey(beforeField)) {
            throw new DataException("Record value has neither a '" + afterField
                    + "' nor a '" + beforeField + "' field; not a CDC envelope?");
        }

        final Object after = envelope.get(afterField);
        final Object before = envelope.get(beforeField);
        final Object state = after != null ? after : before;
        if (state == null) {
            throw new DataException("CDC envelope has neither 'before' nor 'after' state populated.");
        }

        final Map<String, Object> outbox = new HashMap<>();
        outbox.put("id", idOf(record, envelope, state));
        outbox.put("aggregatetype", aggregateTypeOf(record, envelope));
        outbox.put("type", eventTypeOf(envelope, state));
        outbox.put("payload", toJson(record.topic(), null, state));
        if (!timestampField.isEmpty()) {
            outbox.put("timestamp", envelope.get(timestampField));
        }

        // The outbox message carries no key (the original key is folded into the 'id' field).
        return record.newRecord(outboxTopic, record.kafkaPartition(),
                null, null,
                null, outbox, record.timestamp());
    }

    private R applyWithSchema(R record) {
        final Struct envelope = (Struct) record.value();
        final Schema envelopeSchema = record.valueSchema();

        final Field afterFld = envelopeSchema.field(afterField);
        final Field beforeFld = envelopeSchema.field(beforeField);
        if (afterFld == null && beforeFld == null) {
            throw new DataException("Record value schema has neither a '" + afterField
                    + "' nor a '" + beforeField + "' field; not a CDC envelope?");
        }

        final Struct after = afterFld != null ? envelope.getStruct(afterField) : null;
        final Struct before = beforeFld != null ? envelope.getStruct(beforeField) : null;
        final Struct state = after != null ? after : before;
        if (state == null) {
            throw new DataException("CDC envelope has neither 'before' nor 'after' state populated.");
        }

        final Struct outbox = new Struct(outboxSchema);
        outbox.put("id", idOf(record, envelope, state));
        outbox.put("aggregatetype", aggregateTypeOf(record, envelope));
        outbox.put("type", eventTypeOf(envelope, state));
        outbox.put("payload", toJson(record.topic(), state.schema(), state));
        if (!timestampField.isEmpty() && envelopeSchema.field(timestampField) != null) {
            outbox.put("timestamp", envelope.get(timestampField));
        }

        // The outbox message carries no key (the original key is folded into the 'id' field).
        return record.newRecord(outboxTopic, record.kafkaPartition(),
                null, null,
                outboxSchema, outbox, record.timestamp());
    }

    /** Serialize a change-event state to a schemaless JSON string, or null if there is no state. */
    private String toJson(String topic, Schema schema, Object state) {
        if (state == null) {
            return null;
        }
        final byte[] bytes = jsonConverter.fromConnectData(topic, schema, state);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Outbox id. When {@code id.field} is configured, the value of that field in the change-event
     * state is used; otherwise the original record key is joined with the captured table name,
     * e.g. "101.EMPLOYEES".
     */
    private String idOf(R record, Object envelope, Object state) {
        if (!idField.isEmpty()) {
            final Object value = fieldValue(state, idField);
            return value == null ? null : String.valueOf(value);
        }
        final String table = capturedTable(record, envelope);
        final String key = keyString(record.key());
        if (key == null || key.isEmpty()) {
            return table;
        }
        return key + ID_SEPARATOR + table;
    }

    /** Render the record key as a string; composite (Struct/Map) keys join their parts with '_'. */
    private static String keyString(Object key) {
        if (key == null) {
            return null;
        }
        if (key instanceof Struct struct) {
            final StringJoiner joiner = new StringJoiner(KEY_SEPARATOR);
            for (Field field : struct.schema().fields()) {
                final Object value = struct.get(field);
                joiner.add(value == null ? "" : String.valueOf(value));
            }
            return joiner.toString();
        }
        if (key instanceof Map<?, ?> map) {
            // Iterate in field-name order so the joined id is stable across records with the
            // same composite key (a HashMap's own iteration order is not deterministic).
            final StringJoiner joiner = new StringJoiner(KEY_SEPARATOR);
            map.entrySet().stream()
                    .sorted(Comparator.comparing(e -> String.valueOf(e.getKey())))
                    .forEach(e -> joiner.add(e.getValue() == null ? "" : String.valueOf(e.getValue())));
            return joiner.toString();
        }
        return String.valueOf(key);
    }

    private String aggregateTypeOf(R record, Object envelope) {
        return aggregateType.isEmpty() ? capturedTable(record, envelope) : aggregateType;
    }

    /**
     * The captured table name from the envelope's source metadata ({@code source.table}),
     * falling back to the last dot-separated segment of the record topic.
     */
    private String capturedTable(R record, Object envelope) {
        final Object table = fieldValue(fieldValue(envelope, SOURCE_FIELD), TABLE_FIELD);
        if (table != null) {
            return String.valueOf(table);
        }
        return tableFromTopic(record.topic());
    }

    private static String tableFromTopic(String topic) {
        if (topic == null) {
            return null;
        }
        final int dot = topic.lastIndexOf('.');
        return dot >= 0 ? topic.substring(dot + 1) : topic;
    }

    private String eventTypeOf(Object envelope, Object state) {
        if (eventTypeField.isEmpty()) {
            final Object op = fieldValue(envelope, opField);
            return op == null ? null : String.valueOf(op);
        }
        // Look in the envelope first, then fall back to the state.
        Object value = fieldValue(envelope, eventTypeField);
        if (value == null) {
            value = fieldValue(state, eventTypeField);
        }
        return value == null ? null : String.valueOf(value);
    }

    private static Object fieldValue(Object container, String field) {
        if (container == null) {
            return null;
        }
        if (container instanceof Struct struct) {
            return struct.schema().field(field) == null ? null : struct.get(field);
        }
        if (container instanceof Map<?, ?> map) {
            return map.get(field);
        }
        throw new DataException("Unsupported value type: " + container.getClass().getName());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        if (jsonConverter != null) {
            jsonConverter.close();
            jsonConverter = null;
        }
    }
}
