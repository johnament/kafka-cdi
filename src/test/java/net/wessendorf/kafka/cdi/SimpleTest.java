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

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import net.wessendorf.beans.KafkaService;
import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.cdi.extension.KafkaExtension;
import net.wessendorf.kafka.impl.DelegationKafkaConsumer;
import net.wessendorf.kafka.serialization.CafdiSerdes;
import net.wessendorf.kafka.SimpleKafkaProducer;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.enterprise.inject.spi.Extension;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

@RunWith(Arquillian.class)
public class SimpleTest {

    @Inject
    private KafkaService service;

    private KafkaConsumer consumer;
    private KafkaProducer producer;

    private File dataDir;
    private KafkaCluster kafkaCluster;

    @Deployment
    public static JavaArchive createDeployment() {

        return ShrinkWrap.create(JavaArchive.class)
                .addPackage(Consumer.class.getPackage())
                .addPackage(DelegationKafkaConsumer.class.getPackage())
                .addPackage(CafdiSerdes.class.getPackage())
                .addClasses(KafkaExtension.class, SimpleKafkaProducer.class)
                .addClasses(KafkaService.class)
                .addAsServiceProvider(Extension.class, KafkaExtension.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Before
    public void beforeEach() throws IOException {

        dataDir = Testing.Files.createTestingDirectory("cluster");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir)
                .withPorts(2181, 9092);

        kafkaCluster.addBrokers(1).startup();
        kafkaCluster.createTopic("the_topic", 2, 1);


    }

    @After
    public void afterEach() {
        close();
        kafkaCluster.shutdown();
        Testing.Files.delete(dataDir);
    }

    @Test
    public void testSendAndReceive() throws Exception {


        service.sendMessage();


        Properties cconfig = kafkaCluster.useTo().getConsumerProperties("the_consumer", "the_consumer", OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList("the_topic"));

        boolean loop = true;

        while(loop) {

            final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {

                assertThat(record.value()).isEqualTo("This is only a test");
                loop = false;
            }
        }
    }

    @Test
    public void nonNullProducer() {
        assertThat(service.returnProducer()).isNotNull();
    }

    @Test
    public void vanillaKafka() throws IOException {

        Properties pconfig = kafkaCluster.useTo().getProducerProperties("the_producer");
        pconfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pconfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(pconfig);

        producer.send(new ProducerRecord("the_topic", "Hello"));

        Properties cconfig = kafkaCluster.useTo().getConsumerProperties("the_consumer", "the_consumer", OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList("the_topic"));

        final ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {

            assertThat(record.value()).isEqualTo("Hello");
        }
    }

    protected void close() {
        service.returnProducer().closeProducer();

        if (producer != null)
            producer.close();
        if (consumer != null)
            consumer.close();

    }
}
