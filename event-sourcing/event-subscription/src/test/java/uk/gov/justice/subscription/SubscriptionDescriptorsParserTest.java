package uk.gov.justice.subscription;

import static java.nio.file.Paths.get;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionDescriptorDefinition;
import uk.gov.justice.subscription.yaml.parser.YamlFileValidator;
import uk.gov.justice.subscription.yaml.parser.YamlParser;
import uk.gov.justice.subscription.yaml.parser.YamlSchemaLoader;
import uk.gov.justice.subscription.yaml.parser.YamlToJsonObjectConverter;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class SubscriptionDescriptorsParserTest {

    private SubscriptionDescriptorsParser subscriptionDescriptorsParser;

    @Before
    public void setUp() {
        final YamlParser yamlParser = new YamlParser();
        final YamlSchemaLoader yamlSchemaLoader = new YamlSchemaLoader();
        final ObjectMapper objectMapper = new ObjectMapperProducer().objectMapper();
        final YamlFileValidator yamlFileValidator = new YamlFileValidator(new YamlToJsonObjectConverter(yamlParser, objectMapper), yamlSchemaLoader);

        subscriptionDescriptorsParser = new SubscriptionDescriptorsParser(yamlParser, yamlFileValidator);
    }

    @Test
    public void shouldParseSubscriptionDescriptorYamlUrl() throws Exception {
        final URL url = getFromClasspath("yaml/subscription-descriptor.yaml");

        final List<SubscriptionDescriptorDefinition> subscriptionDescriptorDefinitions = subscriptionDescriptorsParser
                .getSubscriptionDescriptorsFrom(singletonList(url))
                .collect(toList());

        assertThat(subscriptionDescriptorDefinitions.size(), is(1));
        assertThat(subscriptionDescriptorDefinitions.get(0).getSubscriptions().size(), is(2));
        assertThat(subscriptionDescriptorDefinitions.get(0).getService(), is("examplecontext"));
        assertThat(subscriptionDescriptorDefinitions.get(0).getServiceComponent(), is("EVENT_LISTENER"));
        assertThat(subscriptionDescriptorDefinitions.get(0).getSpecVersion(), is("1.0.0"));
    }

    @SuppressWarnings("ConstantConditions")
    private URL getFromClasspath(final String name) throws MalformedURLException {
        return get(getClass().getClassLoader().getResource(name).getPath()).toUri().toURL();
    }
}