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
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;

import java.util.List;

@RequiredArgsConstructor
public class TransferTaskFactory implements TaskFactory<FileDownloadRequest> {
    private final TransferTask transferTask;

    @Override
    public Runnable create(List<FileDownloadRequest> records) {
        return () -> {
            if (!records.isEmpty()) {
                transferTask.processBucket(records.get(0).getBucketId(), records);
            }
        };
    }
}
