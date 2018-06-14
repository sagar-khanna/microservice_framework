package uk.gov.justice.services.core.interceptor;

import static java.lang.String.format;
import static uk.gov.justice.services.core.annotation.ComponentNameUtil.componentFrom;
import static uk.gov.justice.services.messaging.logging.LoggerUtils.trace;

import uk.gov.justice.services.core.annotation.ComponentNameUtil;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.dispatcher.DispatcherCache;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped

public class InterceptorChainProcessorProducer {

    @Inject
    Logger logger;

    @Inject
    DispatcherCache dispatcherCache;

    @Inject
    InterceptorCache interceptorCache;

    /**
     * Produces an interceptor chain processor for the provided injection point.
     *
     * @param injectionPoint class where the {@link InterceptorChainProcessor} is being injected
     * @return the interceptor chain processor
     */
    @Produces
    @ServiceComponent("")
    public InterceptorChainProcessor produceProcessor(final InjectionPoint injectionPoint) {
        trace(logger, () -> format("Interceptor Chain Processor provided for %s", injectionPoint.getClass().getName()));

        final String component = componentFrom(injectionPoint);
        return new DefaultInterceptorChainProcessor(interceptorCache, dispatcherCache.dispatcherFor(injectionPoint)::dispatch, component);
    }
}