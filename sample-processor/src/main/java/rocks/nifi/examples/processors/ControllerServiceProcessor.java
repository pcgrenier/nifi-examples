/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rocks.nifi.examples.processors;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import rocks.nifi.examples.PropertiesFileService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author phillip
 */
@SideEffectFree
@Tags({"Properties", "NIFI ROCKS"})
@CapabilityDescription("Fetch value from properties service.")
public class ControllerServiceProcessor extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final String MATCH_ATTR = "match";

    public static final PropertyDescriptor PROPERTY_NAME = new PropertyDescriptor.Builder()
            .name("Property Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTIES_SERVICE = new PropertyDescriptor.Builder()
            .name("Properties Service")
            .description("System properties loader")
            .required(false)
            .identifiesControllerService(PropertiesFileService.class)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROPERTY_NAME);
        properties.add(PROPERTIES_SERVICE);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog log = this.getLogger();
        final AtomicReference<String> value = new AtomicReference<>();

        final String propertyName = context.getProperty(PROPERTY_NAME).getValue();
        final PropertiesFileService propertiesService = context.getProperty(PROPERTIES_SERVICE).asControllerService(PropertiesFileService.class);
        final String property = propertiesService.getProperty(propertyName);
        log.info("Property = " + property);


        FlowFile flowfile = session.get();
        // Write the results to an attribute

        if(property != null && !property.isEmpty()){
            flowfile = session.putAttribute(flowfile, "property", property);
        }

        session.transfer(flowfile, SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

}
