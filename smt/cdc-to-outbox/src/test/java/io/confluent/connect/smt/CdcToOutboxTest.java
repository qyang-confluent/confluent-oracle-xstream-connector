/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package io.confluent.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcToOutboxTest {

    private final CdcToOutbox<SourceRecord> xform = new CdcToOutbox<>();

    @AfterEach
    void tearDown() {
        xform.close();
    }

    // ---- schema-based ----

    private static final Schema STATE_SCHEMA = SchemaBuilder.struct().name("Customer")
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();

    private static final Schema ENVELOPE_SCHEMA = SchemaBuilder.struct().name("Envelope")
            .field("before", STATE_SCHEMA.schema().isOptional() ? STATE_SCHEMA : makeOptional(STATE_SCHEMA))
            .field("after", makeOptional(STATE_SCHEMA))
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private static Schema makeOptional(Schema base) {
        return SchemaBuilder.struct().name(base.name()).optional()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
    }

    @Test
    void reshapesInsertWithSchema() {
        xform.configure(Map.of("aggregate.type", "customer"));

        Struct after = new Struct(ENVELOPE_SCHEMA.field("after").schema())
                .put("id", 42L).put("name", "Ada");
        Struct envelope = new Struct(ENVELOPE_SCHEMA)
                .put("after", after).put("op", "c").put("ts_ms", 1718000000000L);

        Schema keySchema = SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", 42L);

        SourceRecord record = new SourceRecord(null, null, "dbserver.inventory.customers",
                0, keySchema, key, ENVELOPE_SCHEMA, envelope, 123L);

        SourceRecord out = xform.apply(record);
        Struct value = (Struct) out.value();

        assertEquals(CdcToOutbox.OUTBOX_SCHEMA_NAME, out.valueSchema().name());
        assertEquals(Schema.Type.STRING, out.valueSchema().field("payload").schema().type());
        // The original key is folded into 'id'; the outbox record itself has no key.
        assertNull(out.key());
        assertNull(out.keySchema());
        // id = record key joined with the captured table name (last topic segment here).
        assertEquals("42.customers", value.getString("id"));
        assertEquals("customer", value.getString("aggregatetype"));
        assertEquals("c", value.getString("type"));
        assertEquals(1718000000000L, value.getInt64("timestamp"));
        // payload is a schemaless JSON string; field order follows the state schema.
        assertEquals("{\"id\":42,\"name\":\"Ada\"}", value.getString("payload"));
        assertEquals(123L, out.timestamp());
    }

    @Test
    void usesBeforeStateForDelete() {
        xform.configure(Map.of("aggregate.type", "customer"));

        Struct before = new Struct(ENVELOPE_SCHEMA.field("before").schema())
                .put("id", 7L).put("name", "Grace");
        Struct envelope = new Struct(ENVELOPE_SCHEMA)
                .put("before", before).put("op", "d");

        SourceRecord record = new SourceRecord(null, null, "topic", 0,
                Schema.INT64_SCHEMA, 7L, ENVELOPE_SCHEMA, envelope);

        Struct value = (Struct) xform.apply(record).value();
        assertEquals("7.topic", value.getString("id"));
        assertEquals("d", value.getString("type"));
        assertEquals("{\"id\":7,\"name\":\"Grace\"}", value.getString("payload"));
    }

    @Test
    void aggregateTypeDefaultsToTableFromTopic() {
        // No source.table in this envelope, so it falls back to the last topic segment.
        xform.configure(Map.of());

        Struct after = new Struct(ENVELOPE_SCHEMA.field("after").schema())
                .put("id", 1L).put("name", "x");
        Struct envelope = new Struct(ENVELOPE_SCHEMA).put("after", after).put("op", "c");

        SourceRecord record = new SourceRecord(null, null, "cflt.SAMPLE.EMPLOYEES", 0,
                ENVELOPE_SCHEMA, envelope);

        assertEquals("EMPLOYEES", ((Struct) xform.apply(record).value()).getString("aggregatetype"));
    }

    @Test
    void aggregateTypeDefaultsToSourceTable() {
        // When the envelope carries source.table, that wins over the topic-derived name.
        xform.configure(Map.of());

        Schema sourceSchema = SchemaBuilder.struct().name("Source")
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct().name("Envelope")
                .field("after", makeOptional(STATE_SCHEMA))
                .field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema)
                .build();

        Struct after = new Struct(envelopeSchema.field("after").schema())
                .put("id", 5L).put("name", "y");
        Struct source = new Struct(sourceSchema).put("schema", "SAMPLE").put("table", "CUSTOMERS");
        Struct envelope = new Struct(envelopeSchema)
                .put("after", after).put("op", "c").put("source", source);

        SourceRecord record = new SourceRecord(null, null, "cflt.SAMPLE.EMPLOYEES", 0,
                envelopeSchema, envelope);

        assertEquals("CUSTOMERS", ((Struct) xform.apply(record).value()).getString("aggregatetype"));
        // No record key here, so id falls back to the source table (not the topic-derived "EMPLOYEES").
        assertEquals("CUSTOMERS", ((Struct) xform.apply(record).value()).getString("id"));
    }

    @Test
    void idJoinsCompositeKeyWithUnderscore() {
        xform.configure(Map.of());

        Struct after = new Struct(ENVELOPE_SCHEMA.field("after").schema())
                .put("id", 5L).put("name", "y");
        Struct envelope = new Struct(ENVELOPE_SCHEMA).put("after", after).put("op", "c");

        Schema keySchema = SchemaBuilder.struct().name("Key")
                .field("region", Schema.STRING_SCHEMA)
                .field("id", Schema.INT64_SCHEMA)
                .build();
        Struct key = new Struct(keySchema).put("region", "US").put("id", 5L);

        SourceRecord record = new SourceRecord(null, null, "cflt.SAMPLE.ORDERS", 0,
                keySchema, key, ENVELOPE_SCHEMA, envelope, null);

        assertEquals("US_5.ORDERS", ((Struct) xform.apply(record).value()).getString("id"));
    }

    @Test
    void idFieldOverridesKeyAndTable() {
        // With id.field set, the id is that state field's value alone (no key, no table).
        xform.configure(Map.of("id.field", "name"));

        Struct after = new Struct(ENVELOPE_SCHEMA.field("after").schema())
                .put("id", 42L).put("name", "order-a1b2");
        Struct envelope = new Struct(ENVELOPE_SCHEMA).put("after", after).put("op", "c");

        Schema keySchema = SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", 42L);

        SourceRecord record = new SourceRecord(null, null, "cflt.SAMPLE.ORDERS", 0,
                keySchema, key, ENVELOPE_SCHEMA, envelope, null);

        assertEquals("order-a1b2", ((Struct) xform.apply(record).value()).getString("id"));
    }

    // ---- schemaless ----

    @Test
    void reshapesSchemaless() {
        xform.configure(Map.of("aggregate.type", "customer"));

        Map<String, Object> after = new HashMap<>();
        after.put("id", 99);
        after.put("name", "Linus");
        Map<String, Object> envelope = new HashMap<>();
        envelope.put("after", after);
        envelope.put("op", "u");
        envelope.put("ts_ms", 1718000000000L);

        SourceRecord record = new SourceRecord(null, null, "topic", 0, null, 99, null, envelope);

        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) xform.apply(record).value();
        assertNull(xform.apply(record).valueSchema());
        assertEquals("99.topic", value.get("id"));
        assertEquals("customer", value.get("aggregatetype"));
        assertEquals("u", value.get("type"));
        assertEquals(1718000000000L, value.get("timestamp"));
        // payload is a schemaless JSON string (HashMap order is not deterministic).
        String payload = (String) value.get("payload");
        assertTrue(payload.contains("\"id\":99"));
        assertTrue(payload.contains("\"name\":\"Linus\""));
    }

    @Test
    void schemalessCompositeKeyIdIsDeterministic() {
        // A Map key's own iteration order is not stable, so the id must join the parts in a
        // deterministic (field-name) order regardless of insertion order.
        xform.configure(Map.of("aggregate.type", "orders"));

        Map<String, Object> after = new HashMap<>();
        after.put("id", 5);
        Map<String, Object> envelope = new HashMap<>();
        envelope.put("after", after);
        envelope.put("op", "c");

        Map<String, Object> key1 = new HashMap<>();
        key1.put("region", "US");
        key1.put("id", 5);
        Map<String, Object> key2 = new HashMap<>();
        key2.put("id", 5);
        key2.put("region", "US");

        SourceRecord r1 = new SourceRecord(null, null, "topic", 0, null, key1, null, envelope);
        SourceRecord r2 = new SourceRecord(null, null, "topic", 0, null, key2, null, envelope);

        @SuppressWarnings("unchecked")
        String id1 = (String) ((Map<String, Object>) xform.apply(r1).value()).get("id");
        @SuppressWarnings("unchecked")
        String id2 = (String) ((Map<String, Object>) xform.apply(r2).value()).get("id");
        // Sorted by field name: "id" before "region".
        assertEquals("5_US.topic", id1);
        assertEquals(id1, id2);
    }

    // ---- edge cases ----

    @Test
    void passesTombstoneThrough() {
        xform.configure(Map.of());
        SourceRecord record = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", null, null);
        assertSame(record, xform.apply(record));
    }

    @Test
    void usesConfiguredEventTypeField() {
        xform.configure(Map.of(
                "aggregate.type", "customer",
                "event.type.field", "name"));

        Struct after = new Struct(ENVELOPE_SCHEMA.field("after").schema())
                .put("id", 1L).put("name", "CustomerCreated");
        Struct envelope = new Struct(ENVELOPE_SCHEMA).put("after", after).put("op", "c");

        SourceRecord record = new SourceRecord(null, null, "topic", 0, ENVELOPE_SCHEMA, envelope);
        assertEquals("CustomerCreated", ((Struct) xform.apply(record).value()).getString("type"));
    }

    @Test
    void rejectsSchemalessEnvelopeWithNoState() {
        // Aligns schemaless behavior with the schema-based path: a CDC envelope whose
        // 'before' and 'after' are both absent/null is rejected rather than silently
        // producing an outbox record with null id and null payload.
        xform.configure(Map.of());

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("op", "u");
        SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, envelope);

        DataException ex = assertThrows(DataException.class, () -> xform.apply(record));
        assertTrue(ex.getMessage().contains("CDC envelope"));
    }

    @Test
    void rejectsNonEnvelopeSchema() {
        xform.configure(Map.of());
        Schema notEnvelope = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
        Struct value = new Struct(notEnvelope).put("id", 1L);
        SourceRecord record = new SourceRecord(null, null, "topic", 0, notEnvelope, value);

        DataException ex = assertThrows(DataException.class, () -> xform.apply(record));
        assertTrue(ex.getMessage().contains("CDC envelope"));
    }
}
