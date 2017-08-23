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
package org.aerogear.kafka.cdi;

import org.aerogear.kafka.cdi.beans.KafkaService;
import org.aerogear.kafka.cdi.beans.mock.MessageReceiver;
import org.aerogear.kafka.cdi.beans.mock.MockProvider;
import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.aerogear.kafka.cdi.tests.KafkaClusterTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Arquillian.class)
public class ServiceInjectionTest extends KafkaClusterTestBase {

    public static final String TOPIC_NAME = "ServiceInjectionTest";
    private final Logger logger = LoggerFactory.getLogger(ServiceInjectionTest.class);

    @Deployment
    public static JavaArchive createDeployment() {

        return AbstractTestBase.createFrameworkDeployment()
                .addPackage(KafkaService.class.getPackage())
                .addPackage(MockProvider.class.getPackage());
    }

    @Inject
    private KafkaService service;

    @BeforeClass
    public static void createTopic() {
        kafkaCluster.createTopics(TOPIC_NAME);
    }

    @Test
    public void nonNullProducer() {
        Assertions.assertThat(service.returnProducer()).isNotNull();
    }

    @Test
    public void testSendAndReceive(MessageReceiver receiver) throws Exception {

        final String consumerId = TOPIC_NAME;

        Thread.sleep(1000);
        service.sendMessage();

        Properties cconfig = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        boolean loop = true;

        while(loop) {

            final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<String, String> record : records) {
                logger.trace("In polling loop, we got {}", record.value());
                assertThat(record.value()).isEqualTo("This is only a test");
                loop = false;
            }
        }

        Mockito.verify(receiver, Mockito.times(1)).ack();
    }
}
