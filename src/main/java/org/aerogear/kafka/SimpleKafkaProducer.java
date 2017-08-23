/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
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
package org.aerogear.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Simple interface representing a wrapped Apache Kafka Producer, and offering simplified sender methods.
 * @param <K>
 * @param <V>
 */
public interface SimpleKafkaProducer<K, V> {

    Future<RecordMetadata> send(String topic, V payload);
    Future<RecordMetadata> send(String topic, V payload, Callback callback);
    Future<RecordMetadata> send(String topic, K key, V payload);
    Future<RecordMetadata> send(String topic, K key, V payload, Callback callback);
    void closeProducer();
}
