package uk.gov.justice.services.eventsourcing.source.core.spliterator;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class TestEventProvider implements EventProvider {

    private final List<JsonEnvelope> jsonEnvelopes;

    public TestEventProvider(final List<JsonEnvelope> jsonEnvelopes) {
        this.jsonEnvelopes = jsonEnvelopes;
    }

    public Stream<JsonEnvelope> getAllEventsFrom(final long position, final int pageSize) {

        final long listPosition = position - 1;
        final List<JsonEnvelope> subList = new LinkedList<JsonEnvelope>();

        for (int i = (int) listPosition; i < listPosition + pageSize; i++) {
            if (i == jsonEnvelopes.size()) {
                break;
            }

            subList.add(jsonEnvelopes.get(i));
        }

        return subList.stream();
    }
}
