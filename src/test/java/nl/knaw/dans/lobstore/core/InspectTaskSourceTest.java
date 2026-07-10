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

import nl.knaw.dans.lobstore.db.TransferRequestDao;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class InspectTaskSourceTest {

    private final TransferRequestDao transferRequestDao = mock(TransferRequestDao.class);
    private final ActiveTaskRegistry activeTaskRegistry = mock(ActiveTaskRegistry.class);
    private final InspectTaskSource source = new InspectTaskSource(transferRequestDao, activeTaskRegistry);

    @Test
    void nextInput_should_return_first_available_item() {
        UUID id1 = UUID.randomUUID();
        TransferRequest item1 = TransferRequest.builder().id(id1).build();
        
        when(transferRequestDao.findInspectableItems(anyInt())).thenReturn(List.of(item1));
        when(activeTaskRegistry.add(id1)).thenReturn(true);

        Optional<TransferRequest> result = source.nextInput();

        assertThat(result).contains(item1);
        verify(activeTaskRegistry).add(id1);
    }

    @Test
    void nextInput_should_skip_active_items() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        TransferRequest item1 = TransferRequest.builder().id(id1).build();
        TransferRequest item2 = TransferRequest.builder().id(id2).build();

        when(transferRequestDao.findInspectableItems(anyInt())).thenReturn(List.of(item1, item2));
        when(activeTaskRegistry.add(id1)).thenReturn(false);
        when(activeTaskRegistry.add(id2)).thenReturn(true);

        Optional<TransferRequest> result = source.nextInput();

        assertThat(result).contains(item2);
        verify(activeTaskRegistry).add(id1);
        verify(activeTaskRegistry).add(id2);
    }

    @Test
    void nextInput_should_return_empty_if_no_items() {
        when(transferRequestDao.findInspectableItems(anyInt())).thenReturn(List.of());

        Optional<TransferRequest> result = source.nextInput();

        assertThat(result).isEmpty();
    }
}
