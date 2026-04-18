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

import io.dropwizard.hibernate.UnitOfWork;
import io.dropwizard.util.DataSize;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.db.ClaimDao;

import java.util.Map;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class QuotaManager {
    private final ClaimDao claimDao;
    private final Map<String, DataSize> diskSpace;

    @UnitOfWork
    public boolean claim(String id, String target, long size) {
        log.debug("Attempting to claim {} bytes for target '{}' with id '{}'", size, target, id);
        
        Optional<Claim> existingClaim = claimDao.findById(id);
        if (existingClaim.isPresent()) {
            log.warn("Claim with id '{}' already exists", id);
            return false;
        }

        DataSize quota = diskSpace.get(target);
        if (quota == null) {
            throw new IllegalArgumentException("No quota configured for target '" + target + "'");
        }

        long claimedSize = claimDao.sumSizeByTarget(target);
        if (claimedSize + size > quota.toBytes()) {
            log.warn("Not enough unclaimed space for target '{}'. Quota: {}, Claimed: {}, Requested: {}", 
                target, quota.toBytes(), claimedSize, size);
            return false;
        }

        Claim claim = Claim.builder()
            .id(id)
            .target(target)
            .size(size)
            .build();
        
        claimDao.save(claim);
        log.info("Successfully claimed {} bytes for target '{}' with id '{}'", size, target, id);
        return true;
    }

    @UnitOfWork
    public void release(String id, String target) {
        log.debug("Releasing claim with id '{}' for target '{}'", id, target);

        if (!diskSpace.containsKey(target)) {
            throw new IllegalArgumentException("No quota configured for target '" + target + "'");
        }

        claimDao.findById(id).ifPresent(claim -> {
            if (claim.getTarget().equals(target)) {
                claimDao.delete(claim);
                log.info("Released claim with id '{}' for target '{}'", id, target);
            } else {
                log.warn("Claim with id '{}' exists but target '{}' does not match '{}'", id, target, claim.getTarget());
            }
        });
    }
}
