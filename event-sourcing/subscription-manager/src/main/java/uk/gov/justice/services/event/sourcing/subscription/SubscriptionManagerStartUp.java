package uk.gov.justice.services.event.sourcing.subscription;

import uk.gov.justice.services.subscription.SubscriptionManager;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionDescriptorDefinition;
import uk.gov.justice.subscription.registry.SubscriptionDescriptorDefinitionRegistry;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Startup
@Singleton
public class SubscriptionManagerStartUp {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionManagerStartUp.class);

    @Inject
    @Any
    private Instance<SubscriptionManager> subscriptionManagers;

    @Inject
    private SubscriptionDescriptorDefinitionRegistry subscriptionDescriptorDefinitionRegistry;

    @PostConstruct
    public void start() {
        LOGGER.info("SubscriptionManagerStartUp started");
        final Stream<SubscriptionDescriptorDefinition> subscriptionDescriptorDefinitions =
                subscriptionDescriptorDefinitionRegistry.subscriptionDescriptorDefinitions();

        subscriptionDescriptorDefinitions.forEach(
                subscriptionDescriptorDefinition ->
                {
                    final List<Subscription> subscriptions = subscriptionDescriptorDefinition.getSubscriptions();
                    subscriptions.forEach(subscription -> {
                        final SubscriptionNameQualifier subscriptionNameQualifier = new SubscriptionNameQualifier(subscription.getName());
                        final SubscriptionManager subscriptionManager = subscriptionManagers.select(subscriptionNameQualifier).get();
                        subscriptionManager.startSubscription();
                    });
                });
    }
}
