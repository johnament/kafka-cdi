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
package net.wessendorf.kafka.cdi.tests;

import net.wessendorf.kafka.SimpleKafkaProducer;
import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.cdi.extension.KafkaExtension;
import net.wessendorf.kafka.impl.DelegationKafkaConsumer;
import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import javax.enterprise.inject.spi.Extension;

public abstract class AbstractTestBase {

    public static JavaArchive createFrameworkDeployment() {

        return ShrinkWrap.create(JavaArchive.class)
                .addPackage(Consumer.class.getPackage())
                .addPackage(DelegationKafkaConsumer.class.getPackage())
                .addPackage(CafdiSerdes.class.getPackage())
                .addClasses(KafkaExtension.class, SimpleKafkaProducer.class)
                .addAsServiceProvider(Extension.class, KafkaExtension.class)
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }
}