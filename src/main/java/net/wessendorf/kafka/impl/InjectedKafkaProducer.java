/**
 * Copyright (C) 2017 Matthias Wessendorf.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.wessendorf.kafka.impl;

import net.wessendorf.kafka.SimpleKafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Future;

public class InjectedKafkaProducer<K, V> extends org.apache.kafka.clients.producer.KafkaProducer implements SimpleKafkaProducer<K, V> {

    private final String topic;

    public InjectedKafkaProducer(final Map<String, Object> configs, final String topic) {
        super(configs);
        this.topic = topic;
    }


    public Future<RecordMetadata> send(V payload) {
        return this.send(new ProducerRecord(topic, payload));
    }

    public Future<RecordMetadata> send(V payload, Callback callback) {
        return this.send(new ProducerRecord(topic, payload), callback);
    }

    public Future<RecordMetadata> send(K key, V payload) {
        return this.send(new ProducerRecord(topic, key, payload));
    }

    public Future<RecordMetadata> send(K key, V payload, Callback callback) {
        return this.send(new ProducerRecord(topic, key, payload), callback);
    }

    public void closeProducer() {
        super.close();
    }
}