/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.fraunhofer.fit.processors.opcua;

import de.fraunhofer.fit.opcua.OPCUAService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"opc"})
@CapabilityDescription("WriteOPCData the data from ONE node from a OPC UA server.")
public class WriteOPCData extends AbstractProcessor {

    private final AtomicReference<String> tag_name = new AtomicReference<>();
    private final AtomicReference<String> tag_type = new AtomicReference<>();
    private final AtomicReference<String> tag_attribute = new AtomicReference<>();
    private final AtomicReference<String> tag_value = new AtomicReference<>();    


    public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
            .name("OPC UA Service")
            .description("Specifies the OPC UA Service that can be used to access data")
            .required(true)
            .identifiesControllerService(OPCUAService.class)
            .sensitive(false)
            .build();

   

    public static final PropertyDescriptor TAG_NAME = new PropertyDescriptor
            .Builder().name("Tag Name")
            .description("The Tag name to be written")
            .required(true)            
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TAG_TYPE = new PropertyDescriptor
            .Builder().name("Tag Type")
            .description("Tag Type")
            .required(true)
            .allowableValues("Boolean", "Byte", "Int16","UInt16","Int32","UInt32","Int64","UInt64","Float", "Double", "String")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("Int32")
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TAG_VALUE_ATTRIBUTE = new PropertyDescriptor
            .Builder().name("Tag Value Attribute")
            .description("The property from the value will be taken")
            .required(false)
            .sensitive(false)            
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .build();

    public static final PropertyDescriptor TAG_VALUE = new PropertyDescriptor
            .Builder().name("Tag Value")
            .description("Static value to be used as default")
            .required(true)
            .sensitive(false)            
            .defaultValue("0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(TAG_NAME);
        descriptors.add(TAG_TYPE);
        descriptors.add(TAG_VALUE_ATTRIBUTE);
        descriptors.add(TAG_VALUE);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        tag_name.set(context.getProperty(TAG_NAME).getValue());
        tag_value.set(context.getProperty(TAG_VALUE).getValue());
        tag_type.set(context.getProperty(TAG_TYPE).getValue());
        tag_attribute.set(context.getProperty(TAG_VALUE_ATTRIBUTE).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Submit to getValue
        OPCUAService opcUAService;

        FlowFile flowFile;
        flowFile = session.get();
        if (flowFile == null)
            flowFile = session.create();


        try {
            opcUAService = context.getProperty(OPCUA_SERVICE).asControllerService(OPCUAService.class);
        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            session.transfer(flowFile);
            context.yield();
            return;
        }

            
        boolean response = false;
        try {
            String valToWrite = flowFile.getAttribute(tag_attribute.get()).toString(); 
            if(valToWrite == null)
            {
                response = opcUAService.writeValue(tag_name.get(),tag_type.get(), tag_value.get());
            }
            else
            {                
                response = opcUAService.writeValue(tag_name.get(),tag_type.get(), valToWrite);
            }
                                                
        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            return;
        }


        byte[] payload = "".getBytes();    
        session.putAttribute(flowFile, "WriteOpereationResult", String.valueOf(response));

        // Write the results back out to flow file
        try {
            flowFile = session.write(flowFile, out -> out.write(payload));
            session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
            getLogger().error("Unable to process", ex);
            session.transfer(flowFile, FAILURE);
        }
    }
}
