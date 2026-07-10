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

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskSource;
import nl.knaw.dans.lobstore.db.TransferRequestDao;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Returns {@link TransferRequest}s that are ready for download. The next inputs are ordered from older to newer.
 */
@RequiredArgsConstructor
public class DownloadTaskSource implements TaskSource<TransferRequest> {
    private static final String TARGET_DOWNLOAD = "download";

    private final TransferRequestDao transferRequestDao;
    private final QuotaManager quotaManager;
    private final ActiveTaskRegistry activeTaskRegistry;
    private final long margin;
    private final int maxConcurrentDownloads;

    @Override
    public List<TransferRequest> nextInputs() {
        var scheduled = new ArrayList<TransferRequest>();
        // Fetch a window of the oldest downloadable items (INSPECTED or already DOWNLOADING). Items that are already
        // being processed are skipped by the ActiveTaskRegistry, so the window naturally caps concurrency at
        // maxConcurrentDownloads while still resuming items left DOWNLOADING after a crash.
        try {
            for (var item : transferRequestDao.findDownloadableItems(maxConcurrentDownloads)) {
                claim(item).ifPresent(scheduled::add);
            }
            return scheduled;
        }
        catch (Exception e) {
            // The batch runs in one @UnitOfWork; on abort the DB rolls back but the in-memory registry does not.
            // Release everything claimed in this batch so those items can be retried on the next poll.
            scheduled.forEach(item -> activeTaskRegistry.remove(item.getId()));
            throw e;
        }
    }

    @Override
    public Optional<TransferRequest> nextInput() {
        // Retained for the TaskSource interface; the PollingTaskExecutor calls nextInputs().
        return nextInputs().stream().findFirst();
    }

    private Optional<TransferRequest> claim(TransferRequest item) {
        if (activeTaskRegistry.add(item.getId())) {
            try {
                if (quotaManager.ensureClaimed(item.getId() + "/base", TARGET_DOWNLOAD, item.getFileSize())) {
                    // TODO: the extra claim is only necessary if downloading in chunks, so if the filesize exceeds the chunk size.
                    if (quotaManager.ensureClaimed(item.getId() + "/extra", TARGET_DOWNLOAD, item.getFileSize() + margin)) {
                        item.setStatus(TransferRequestStatus.DOWNLOADING);
                        transferRequestDao.save(item);
                        return Optional.of(item);
                    }
                    // DO NOT DELETE: IMPORTANT EXPLANATION FOR FUTURE MAINTENANCE.
                    // else {
                    // We DO NOT release the base claim because the same items will be selected in the next try. If the selection
                    // were to become non-deterministic, we would need to release the base claim here to prevent a leak
                    // }
                }
                // If we couldn't proceed (e.g., quota check failed), we remove it from the registry so it can be picked up again.
                activeTaskRegistry.remove(item.getId());
            }
            catch (Exception e) {
                // A failure after claiming (e.g. save throws) must also release the registry entry, otherwise the item
                // stays "active" forever and is skipped on every future poll.
                activeTaskRegistry.remove(item.getId());
                throw e;
            }
        }
        return Optional.empty();
    }
}
