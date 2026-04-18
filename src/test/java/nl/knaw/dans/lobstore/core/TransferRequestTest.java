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
        assertTrue(TransferRequest.builder().status(TransferStatus.PENDING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.INSPECTING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.INSPECTED).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.DOWNLOADING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.DOWNLOADED).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.PACKAGING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.PACKAGED).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.TRANSFERRING).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.TRANSFERRED).build().isInProgress());
        assertTrue(TransferRequest.builder().status(TransferStatus.VERIFYING).build().isInProgress());
    }

    @Test
    void is_in_progress_should_return_false_for_final_statuses() {
        assertFalse(TransferRequest.builder().status(TransferStatus.DONE).build().isInProgress());
        assertFalse(TransferRequest.builder().status(TransferStatus.REJECTED).build().isInProgress());
        assertFalse(TransferRequest.builder().status(TransferStatus.FAILED).build().isInProgress());
    }
}
