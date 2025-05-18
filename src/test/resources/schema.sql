CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

create schema if not exists event_routing;

create table event_routing.message_producers
(
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    producer_name       VARCHAR(255) NOT NULL UNIQUE,
    broker_type         VARCHAR(255) NOT NULL CHECK (message_producers.broker_type IN ('RABBIT', 'KAFKA', 'SQS')),
    key_format          VARCHAR(255) CHECK (message_producers.key_format IN ('STRING', 'JSON', 'AVRO')),
    value_format        VARCHAR(255) NOT NULL CHECK (message_producers.value_format IN ('STRING', 'JSON', 'AVRO')),
    -- Would be JSONB, but we need to encrypt the configuration, so its best to store as an encrypted string
    enc_producer_config TEXT         NOT NULL,
    -- Contains non-sensitive broker configuration
    producer_config     JSONB        NOT NULL,
    created_at          TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);

create index idx_message_producers_producer_name on event_routing.message_producers (producer_name);

create table event_routing.dispatch_topic
(
    id                UUID primary key default uuid_generate_v4(),
    source_topic      VARCHAR(255) not null,
    destination_topic VARCHAR(255) not null,
    key_format        VARCHAR(255) not null check (dispatch_topic.key_format IN ('STRING', 'JSON', 'AVRO')),
    key_schema        TEXT,
    value_format      VARCHAR(255) not null check (dispatch_topic.value_format IN ('STRING', 'JSON', 'AVRO')),
    value_schema      TEXT
);

create index idx_dispatch_source_topic_name on event_routing.dispatch_topic (source_topic);

alter table event_routing.dispatch_topic
    add constraint dispatch_topic_source_topic_destination_topic unique (source_topic, destination_topic);

create table if not exists event_routing.event_listener
(
    id                    UUID primary key      default uuid_generate_v4(),
    topic_name            VARCHAR(255) NOT NULL,
    group_id              VARCHAR(255) NOT NULL,
    run_on_startup        BOOLEAN      NOT NULL DEFAULT false,
    key_format            VARCHAR(255) NOT NULL CHECK ( event_listener.key_format IN ('STRING', 'AVRO', 'JSON') ),
    value_format          VARCHAR(255) NOT NULL CHECK ( event_listener.value_format IN ('STRING', 'AVRO', 'JSON') ),
    additional_properties JSONB,
    created_at            TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

alter table event_routing.event_listener
    add constraint event_listener_topic_name_group_id_unique unique (topic_name, group_id);

create index if not exists event_listener_topic_name_idx
    on event_routing.event_listener (topic_name);
