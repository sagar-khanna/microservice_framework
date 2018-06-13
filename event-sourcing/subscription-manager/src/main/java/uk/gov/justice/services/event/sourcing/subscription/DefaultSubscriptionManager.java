package uk.gov.justice.services.event.sourcing.subscription;

import static uk.gov.justice.services.core.interceptor.InterceptorContext.interceptorContextWithInput;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSubscriptionManager implements SubscriptionManager {
    private final Logger LOGGER = LoggerFactory.getLogger(DefaultSubscriptionManager.class);
    private final Subscription subscription;
    private final EventSource eventSource;

    public DefaultSubscriptionManager(final Subscription subscription,
                                      final EventSource eventSource) {
        this.subscription = subscription;
        this.eventSource = eventSource;
    }

    @Override
    public void process(final JsonEnvelope jsonEnvelope, final InterceptorChainProcessor interceptorChainProcessor) {
        interceptorChainProcessor.process(interceptorContextWithInput(jsonEnvelope));
    }

    @Override
    public void startSubscription() {
        LOGGER.info("Starting subscription: " + subscription.getName() + " for event source: " + subscription.getEventSourceName());
    }
}