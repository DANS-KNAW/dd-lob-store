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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransferRequestTest {

    @Test
    void is_in_progress_should_return_true_for_progressing_statuses() {
        assertTrue(TransferRequest.builder().status(TransferRequestStatus.PENDING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferRequestStatus.INSPECTED).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferRequestStatus.DOWNLOADING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferRequestStatus.DOWNLOADED).build().isInProgress());
        
        Bucket bucket = Bucket.builder().status(BucketStatus.PACKAGING).build();
        assertTrue(TransferRequest.builder().bucket(bucket).build().isInProgress());
    }

    @Test
    void is_in_progress_should_return_false_for_final_statuses() {
        assertFalse(TransferRequest.builder().status(TransferRequestStatus.REJECTED).build().isInProgress());
        assertFalse(TransferRequest.builder().status(TransferRequestStatus.FAILED).build().isInProgress());

        Bucket bucket = Bucket.builder().status(BucketStatus.DONE).build();
        assertFalse(TransferRequest.builder().bucket(bucket).build().isInProgress());
    }
}
