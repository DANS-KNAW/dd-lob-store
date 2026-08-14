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
package nl.knaw.dans.lobstore.resources;

import io.dropwizard.hibernate.UnitOfWork;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lobstore.core.ActiveTaskRegistry;
import nl.knaw.dans.lobstore.core.Bucket;
import nl.knaw.dans.lobstore.core.BucketStatus;
import nl.knaw.dans.lobstore.core.QuotaManager;
import nl.knaw.dans.lobstore.core.TransferRequest;
import nl.knaw.dans.lobstore.db.BucketDao;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class DatastationsResource implements DatastationsApi {
    private static final String TARGET_UPLOAD = "upload";

    private final TransferRequestDao transferRequestDao;
    private final BucketDao bucketDao;
    private final QuotaManager quotaManager;
    private final long minimalBucketSize;
    private final long margin;

    @Override
    @UnitOfWork
    public Response flushTransfers(@NotNull String datastation) {
        List<TransferRequest> items = transferRequestDao.findPackagableItemsByDatastation(datastation);

        if (items.isEmpty()) {
            return Response.noContent().build();
        }

        long currentTotalSize = items.stream()
            .mapToLong(TransferRequest::getFileSize)
            .sum();

        long sizeToClaim = Math.max(currentTotalSize, minimalBucketSize);

        UUID bucketId = UUID.randomUUID();
        var baseClaimId = bucketId + "/base";
        var extraClaimId = bucketId + "/extra";

        if (quotaManager.ensureClaimed(baseClaimId, TARGET_UPLOAD, sizeToClaim)) {
            if (quotaManager.ensureClaimed(extraClaimId, TARGET_UPLOAD, sizeToClaim + margin)) {
                Bucket bucket = Bucket.builder()
                    .id(bucketId)
                    .status(BucketStatus.PACKAGING)
                    .datastation(datastation)
                    .build();
                bucketDao.save(bucket);

                for (var item : items) {
                    item.setBucket(bucket);
                    transferRequestDao.save(item);
                }

                log.info("Flushed {} transfers for datastation {} into new bucket {}", items.size(), datastation, bucketId);
                return Response.accepted().build();
            }
            quotaManager.release(baseClaimId, TARGET_UPLOAD);
        }

        log.warn("Failed to claim quota for flushing transfers of datastation {}", datastation);
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
            .entity("Insufficient quota to start packaging")
            .build();
    }
}
