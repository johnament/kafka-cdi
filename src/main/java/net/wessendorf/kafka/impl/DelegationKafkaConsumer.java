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

import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class DelegationKafkaConsumer implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(DelegationKafkaConsumer.class);

    /*
     * True if a consumer is running; otherwise false
     */
    private final AtomicBoolean running = new AtomicBoolean(true);

    private Object consumerInstance;
    final Properties properties = new Properties();
    final private KafkaConsumer<String, String> consumer;
    final private String topic;
    private final AnnotatedMethod annotatedListenerMethod;
    private final BeanManager beanManager;

    public DelegationKafkaConsumer(final String bootstrapServers, final AnnotatedMethod annotatedMethod,
            final BeanManager beanManager) {
        final Consumer consumerAnnotation = annotatedMethod.getAnnotation(Consumer.class);
        this.topic = consumerAnnotation.topic();
        final String groupId = consumerAnnotation.groupId();
        final Class<?> keyType = consumerAnnotation.keyType();

        this.annotatedListenerMethod = annotatedMethod;
        this.beanManager = beanManager;

        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(GROUP_ID_CONFIG, groupId);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG,
                CafdiSerdes.serdeFrom(keyType(keyType, annotatedMethod)).deserializer().getClass());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG,
                CafdiSerdes.serdeFrom(valueType(annotatedMethod)).deserializer().getClass());

        consumer = new KafkaConsumer(properties);
    }

    private Class<?> keyType(final Class<?> defaultKeyType, final AnnotatedMethod annotatedMethod) {

        if (annotatedMethod.getJavaMember().getParameterTypes().length == 2) {
            return annotatedMethod.getJavaMember().getParameterTypes()[0];
        } else {
            return defaultKeyType;
        }
    }

    private Class<?> valueType(final AnnotatedMethod annotatedMethod) {

        if (annotatedMethod.getJavaMember().getParameterTypes().length == 2) {
            return annotatedMethod.getJavaMember().getParameterTypes()[1];
        } else {
            return annotatedMethod.getJavaMember().getParameterTypes()[0];
        }
    }

    public void initialize() {

        final Set<Bean<?>> beans = beanManager.getBeans(annotatedListenerMethod.getJavaMember().getDeclaringClass());
        final Bean<?> propertyResolverBean = beanManager.resolve(beans);
        final CreationalContext<?> creationalContext = beanManager.createCreationalContext(propertyResolverBean);

        consumerInstance = beanManager.getReference(propertyResolverBean,
                annotatedListenerMethod.getJavaMember().getDeclaringClass(), creationalContext);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topic));
            logger.trace("subscribed to {}", topic);
            while (isRunning()) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        logger.trace("dispatching payload {} to consumer", record.value());

                        if (annotatedListenerMethod.getJavaMember().getParameterTypes().length == 2) {
                            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.key(), record.value());

                        } else {
                            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.value());
                        }

                        logger.trace("dispatched payload {} to consumer", record.value());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        logger.error("error dispatching received value to consumer", e);
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (isRunning()) {
                logger.trace("Exception", e);
                throw e;
            }
        } finally {
            logger.error("Close the consumer.");
            consumer.close();
        }
    }

    /**
     * True when a consumer is running; otherwise false
     */
    public boolean isRunning() {
        return running.get();
    }

    /*
     * Shutdown hook which can be called from a separate thread.
     */
    public void shutdown() {
        logger.info("Shutting down the consumer.");
        running.set(false);
        consumer.wakeup();
    }

}
