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
package net.wessendorf.kafka.cdi;

import net.wessendorf.kafka.SimpleKafkaProducer;
import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.cdi.extension.KafkaExtension;
import net.wessendorf.kafka.cdi.tests.AbstractTestBase;
import net.wessendorf.kafka.cdi.tests.KafkaClusterTestBase;
import net.wessendorf.kafka.impl.DelegationKafkaConsumer;
import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Arquillian.class)
public class VanillaKafkaTest extends KafkaClusterTestBase {

    private Logger logger = LoggerFactory.getLogger(VanillaKafkaTest.class);

    @Deployment
    public static JavaArchive createDeployment() {

        return AbstractTestBase.createFrameworkDeployment();
    }


    @Test
    public void vanillaKafka() throws IOException, InterruptedException {

        final String topicName = "vanillaKafka";
        final String producerId = topicName;
        final String consumerId = topicName;
        kafkaCluster.createTopic(topicName, 1, 1);



        Properties pconfig = kafkaCluster.useTo().getProducerProperties(producerId);
        pconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(pconfig);

        producer.send(new ProducerRecord(topicName, "Hello"));

        Properties cconfig = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(topicName));

        final CountDownLatch latch = new CountDownLatch(1);
        final ConsumerRecords<String, String> records = consumer.poll(1000);
        for (final ConsumerRecord<String, String> record : records) {
            assertThat(record.value()).isEqualTo("Hello");
            latch.countDown();
        }

        final boolean done = latch.await(2, TimeUnit.SECONDS);
        assertThat(done).isTrue();
    }
}
