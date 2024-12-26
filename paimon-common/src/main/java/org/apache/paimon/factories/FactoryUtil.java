/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.factories;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.util.List;
import java.util.stream.Collectors;

/** Utility for working with {@link Factory}s. */
public class FactoryUtil extends BaseFactoryUtil {

    private static final Cache<ClassLoader, List<Factory>> FACTORIES =
            Caffeine.newBuilder().softValues().maximumSize(100).executor(Runnable::run).build();

    /** Discovers a factory using the given factory base class and identifier. */
    @SuppressWarnings("unchecked")
    public static <T extends Factory> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String identifier) {
        final List<Factory> factories = getFactories(classLoader);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.identifier().equals(identifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            identifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::identifier)
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new FactoryException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            identifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    public static <T extends Factory> List<String> discoverIdentifiers(
            ClassLoader classLoader, Class<T> factoryClass) {
        final List<Factory> factories = getFactories(classLoader);

        return factories.stream()
                .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .map(Factory::identifier)
                .collect(Collectors.toList());
    }

    private static List<Factory> getFactories(ClassLoader classLoader) {
        return FACTORIES.get(classLoader, s -> discoverFactories(classLoader, Factory.class));
    }
}
