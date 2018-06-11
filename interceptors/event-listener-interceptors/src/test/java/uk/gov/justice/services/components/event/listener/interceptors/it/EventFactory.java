package uk.gov.justice.services.components.event.listener.interceptors.it;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static uk.gov.justice.services.test.utils.core.messaging.JsonEnvelopeBuilder.envelope;
import static uk.gov.justice.services.test.utils.core.messaging.MetadataBuilderFactory.metadataWithRandomUUID;

import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class EventFactory {


    private final int numberOfStreams;
    private final int numberOfUniqueEventNames;

    private final Random random = new Random();
    private final Map<UUID, List<JsonEnvelope>> eventsByStream = new HashMap<>();

    public EventFactory(final int numberOfStreams, final int numberOfUniqueEventNames) {
        this.numberOfStreams = numberOfStreams;
        this.numberOfUniqueEventNames = numberOfUniqueEventNames;
    }

    public List<JsonEnvelope> generateEvents(final int numberOfEventsToCreate) {

        final List<UUID> streamIds = generateStreamIds();
        final List<String> eventNames = generateEventNames();

        streamIds.forEach(streamId -> eventsByStream.put(streamId, new ArrayList<>()));

        return IntStream.range(0, numberOfEventsToCreate)
                .mapToObj(index -> generateEnvelope(streamIds, eventNames))
                .collect(toList());
    }

    private List<UUID> generateStreamIds() {
        return range(0, numberOfStreams)
                .mapToObj(index -> randomUUID())
                .collect(toList());
    }

    private List<String> generateEventNames() {
        return range(0, numberOfUniqueEventNames)
                .mapToObj(index -> "context.event_" + (index + 1))
                .collect(toList());
    }

    private UUID getARandomStreamId(final List<UUID> streamIds) {

        final int index = random.nextInt(streamIds.size());

        return streamIds.get(index);
    }

    private String getARandomEventName(final List<String> eventNames) {

        final int index = random.nextInt(eventNames.size());

        return eventNames.get(index);
    }

    private JsonEnvelope generateEnvelope(final List<UUID> streamIds, final List<String> eventNames) {

        final UUID streamId = getARandomStreamId(streamIds);
        final String eventName = getARandomEventName(eventNames);

        final List<JsonEnvelope> jsonEnvelopesByStream = eventsByStream.get(streamId);
        final int version = jsonEnvelopesByStream.size() + 1;

        final JsonEnvelope jsonEnvelope = generateJsonEnvelope(streamId, eventName, version);

        jsonEnvelopesByStream.add(jsonEnvelope);

        return jsonEnvelope;
    }

    private JsonEnvelope generateJsonEnvelope(final UUID streamId, final String eventName, final int version) {
        return envelope()
                .with(metadataWithRandomUUID(eventName)
                        .withStreamId(streamId)
                        .withVersion(version)
                        .withSource("test_source")
                )
                .build();
    }
}
