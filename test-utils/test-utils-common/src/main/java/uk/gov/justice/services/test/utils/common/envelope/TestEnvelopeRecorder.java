package uk.gov.justice.services.test.utils.common.envelope;


import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.LinkedList;
import java.util.List;

/**
 * To be used in unit and integration tests for testing invocations of handlers, senders etc..
 */
public abstract class TestEnvelopeRecorder {
    private final List<JsonEnvelope> recordedEnvelopes = new LinkedList<>();

    public void reset() {
        recordedEnvelopes.clear();
    }

    public JsonEnvelope firstRecordedEnvelope() {
        return !recordedEnvelopes.isEmpty() ? recordedEnvelopes.get(0) : null;
    }

    public List<JsonEnvelope> recordedEnvelopes() {
        return recordedEnvelopes;
    }

    protected synchronized void record(final JsonEnvelope envelope) {
        recordedEnvelopes.add(envelope);
    }

}
