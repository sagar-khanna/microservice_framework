package uk.gov.justice.services.example.cakeshop.query.api;


import static uk.gov.justice.services.core.annotation.Component.QUERY_API;

import uk.gov.justice.services.core.annotation.Handles;
import uk.gov.justice.services.core.annotation.ServiceComponent;
import uk.gov.justice.services.core.requester.Requester;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;

@ServiceComponent(QUERY_API)
public class CakeOrdersQueryApi {

    @Inject
    Requester requester;

    @Handles("example.get-order")
    public JsonEnvelope getOrder(final JsonEnvelope query) {
        return requester.request(query);
    }
}
