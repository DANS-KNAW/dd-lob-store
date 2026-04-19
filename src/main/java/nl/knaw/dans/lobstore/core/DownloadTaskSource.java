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

import java.util.List;

/**
 * Returns {@link TransferRequest}s that are ready for download. The next inputs are ordered from older to newer.
 */
@RequiredArgsConstructor
public class DownloadTaskSource implements TaskSource<TransferRequest> {
    private static final String TARGET_DOWNLOAD = "download";

    private final TransferRequestDao transferRequestDao;
    private final QuotaManager quotaManager;
    private final long margin;

    @Override
    public List<TransferRequest> nextInputs() {
        var optItem = transferRequestDao.findNextDownloadableItem();
        if (optItem.isPresent()) {
            var item = optItem.get();
            if (quotaManager.ensureClaimed(item.getId() + "/base", TARGET_DOWNLOAD, item.getFileSize())) {
                if (quotaManager.ensureClaimed(item.getId() + "/extra", TARGET_DOWNLOAD, item.getFileSize() + margin)) {
                    item.setStatus(TransferStatus.DOWNLOADING);
                    transferRequestDao.save(item);
                    return List.of(item);
                }
                //else {
                /*
                 * Since the next downloadable item will be the same one in the next try, we keep the base claim around.
                 * If the selection criteria were to change, we would need to release the base claim here to make sure it
                 * is not leaked.
                 */
                //}
            }
        }
        return List.of();
    }
}
