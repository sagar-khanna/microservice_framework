package uk.gov.justice.services.event.sourcing.subscription;


import uk.gov.justice.services.core.annotation.ServiceComponent;

import java.util.Objects;

import javax.enterprise.util.AnnotationLiteral;

public class ServiceComponentLiteral extends AnnotationLiteral<ServiceComponent> implements ServiceComponent {

    private final String value;

    public ServiceComponentLiteral(final String value) {
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        final ServiceComponentLiteral that = (ServiceComponentLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }
}