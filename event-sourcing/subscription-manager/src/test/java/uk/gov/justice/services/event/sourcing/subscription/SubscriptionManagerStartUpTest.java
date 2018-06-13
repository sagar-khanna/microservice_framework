package uk.gov.justice.services.event.sourcing.subscription;

import static org.codehaus.groovy.runtime.InvokerHelper.asList;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.subscription.SubscriptionManager;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionDescriptorDefinition;
import uk.gov.justice.subscription.registry.SubscriptionDescriptorDefinitionRegistry;

import java.util.stream.Stream;

import javax.enterprise.inject.Instance;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionManagerStartUpTest {


    @Mock(answer = RETURNS_DEEP_STUBS)
    private Instance<SubscriptionManager> subscriptionManagers;

    @Mock
    private SubscriptionDescriptorDefinitionRegistry subscriptionDescriptorDefinitionRegistry;

    @InjectMocks
    private SubscriptionManagerStartUp subscriptionManagerStartUp;

    @Test
    public void shouldStartSubscription() throws Exception {

        final Subscription subscription1 = mock(Subscription.class);

        final SubscriptionDescriptorDefinition subscriptionDescriptorDefinition_1 = mock(SubscriptionDescriptorDefinition.class);

        final SubscriptionManager subscriptionManager1 = mock(SubscriptionManager.class);

        when(subscriptionDescriptorDefinitionRegistry.subscriptionDescriptorDefinitions()).thenReturn(Stream.of(subscriptionDescriptorDefinition_1));

        when(subscriptionDescriptorDefinition_1.getSubscriptions()).thenReturn(asList(subscription1));

        when(subscriptionManagers.select(any(SubscriptionNameQualifier.class)).get()).thenReturn(subscriptionManager1);

        subscriptionManagerStartUp.start();

        verify(subscriptionManager1).startSubscription();
    }


}