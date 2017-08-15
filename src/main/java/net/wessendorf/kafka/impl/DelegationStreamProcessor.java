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

import net.wessendorf.kafka.cdi.annotation.KafkaStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class DelegationStreamProcessor {

    private final Logger logger = LoggerFactory.getLogger(DelegationStreamProcessor.class);
    final Properties properties = new Properties();
    private AnnotatedMethod annotatedProcessorMethod;
    private final KStreamBuilder builder = new KStreamBuilder();
    private KafkaStreams streams;

    public void init(final String bootstrapServers, final AnnotatedMethod annotatedMethod, final BeanManager beanManager) {

        this.annotatedProcessorMethod = annotatedMethod;
        final KafkaStream streamAnnotation = annotatedMethod.getAnnotation(KafkaStream.class);

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "mw-cafdi-" + UUID.randomUUID().toString());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000L);

        final StreamsConfig cfg = new StreamsConfig(properties);


        final KStream<String, String> source = builder.stream(streamAnnotation.input());
        KStream<String, Long> sink = null;

        // wire method and execute it:
        final Set<Bean<?>> beans = beanManager.getBeans(annotatedProcessorMethod.getJavaMember().getDeclaringClass());
        final Bean<?> propertyResolverBean = beanManager.resolve(beans);
        final CreationalContext<?> creationalContext = beanManager.createCreationalContext(propertyResolverBean);

        final Object processorInstance = beanManager.getReference(propertyResolverBean,
                annotatedProcessorMethod.getJavaMember().getDeclaringClass(), creationalContext);

        try {
            sink = (KStream) annotatedProcessorMethod.getJavaMember().invoke(processorInstance, source);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("error dispatching received value to consumer", e);
        }

        sink.through(Serdes.String(), Serdes.Long(), streamAnnotation.output());

        // go!
        try {
            streams = new KafkaStreams(builder, cfg);
            streams.setStateListener((newState, oldState) -> {
                logger.trace("OLD STATE {}", oldState);
                logger.trace("NEW STATE {}", newState);
            });
            streams.start();
        } catch (Exception e) {
            logger.error("Could not start Kafka streaming client", e);
        }
    }
}
