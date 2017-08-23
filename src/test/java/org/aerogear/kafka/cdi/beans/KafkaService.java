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
package org.aerogear.kafka.cdi.beans;

import org.aerogear.kafka.cdi.ServiceInjectionTest;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaConfig(bootstrapServers = "#{KAFKA_SERVICE_HOST}")
public class KafkaService {

    Logger logger = LoggerFactory.getLogger(KafkaService.class);

    @Producer
    private SimpleKafkaProducer<Integer, String> producer;

    public SimpleKafkaProducer returnProducer() {
        return producer;
    }

    public void sendMessage() {
        logger.info("sending message to the topic....");
        producer.send(ServiceInjectionTest.TOPIC_NAME, "This is only a test");
    }

}
