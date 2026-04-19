/*
 * Copyright (C) 2026 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.lobstore.core;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A registry of active transfer IDs that are currently being processed.
 * This is used to prevent multiple concurrent tasks from being created for the same transfer request.
 */
public class ActiveTaskRegistry {
    private final Set<UUID> activeIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Adds a UUID to the registry of active IDs.
     *
     * @param id the identifier of the transfer request to be added to the active registry
     * @return {@code true} if the ID was successfully added to the registry,
     *         {@code false} if the ID is already present
     */
    public boolean add(UUID id) {
        return activeIds.add(id);
    }

    /**
     * Removes a UUID from the registry of active IDs.
     *
     * @param id the identifier of the transfer request to be removed from the active registry
     */
    public void remove(UUID id) {
        activeIds.remove(id);
    }
}
