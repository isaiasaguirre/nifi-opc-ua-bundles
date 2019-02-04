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
package de.fraunhofer.fit.opcua;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscriptionManager;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.DataChangeTrigger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.*;
import org.eclipse.milo.opcua.stack.core.util.CertificateUtil;

import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@Tags({"opc"})
@CapabilityDescription("ControllerService implementation of OPCUAService.")
public class StandardOPCUAService extends AbstractControllerService implements OPCUAService {

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("Endpoint URL")
            .description("the opc.tcp address of the opc ua server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SECURITY_POLICY = new PropertyDescriptor
            .Builder().name("Security Policy")
            .description("What security policy to use for connection with OPC UA server")
            .required(true)
            .allowableValues("None", "Basic128Rsa15", "Basic256", "Basic256Rsa256")
            .defaultValue("None")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURITY_MODE = new PropertyDescriptor
            .Builder().name("Security Mode")
            .description("What security mode to use for connection with OPC UA server. Only valid when \"Security Policy\" isn't \"None\".")
            .required(true)
            .allowableValues("Sign", "SignAndEncrypt")
            .defaultValue("Sign")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_KS_LOCATION = new PropertyDescriptor
            .Builder().name("Client Keystore Location")
            .description("The location of the client keystore. Only valid when \"Security Policy\" isn't \"None\". " +
                    "The keystore should contain only one keypair entry (private key + certificate). " +
                    "If multiple entries exist, the first one is used. " +
                    "Besides, the key should have the same password as the keystore.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CLIENT_KS_PASSWORD = new PropertyDescriptor
            .Builder().name("Client Keystore Password")
            .description("The password for the client keystore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor REQUIRE_SERVER_AUTH = new PropertyDescriptor
            .Builder().name("Require server authentication")
            .description("Whether to authenticate server by verifying its certificate against the trust store. It is recommended to disable this option for quick test, but enable it for production.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRUSTSTORE_LOCATION = new PropertyDescriptor
            .Builder().name("Trust store Location")
            .description("The location of the trust store. Only valid when \"Security Policy\" isn't \"None\". " +
                    "Trust store contains trusted certificates, which are to be used for server identity verification." +
                    "The trust store can contain multiple certificates.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor TRUSTSTORE_PASSWORD = new PropertyDescriptor
            .Builder().name("Trust store Password")
            .description("The password for the trust store")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor AUTH_POLICY = new PropertyDescriptor
            .Builder().name("Authentication Policy")
            .description("How should Nifi authenticate with the UA server")
            .required(true)
            .defaultValue("Anon")
            .allowableValues("Anon", "Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder().name("User Name")
            .description("The user name to access the OPC UA server (only valid when \"Authentication Policy\" is \"Username\")")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .description("The password to access the OPC UA server (only valid when \"Authentication Policy\" is \"Username\")")
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USE_PROXY = new PropertyDescriptor
            .Builder().name("Use Proxy")
            .description("If true, the \"Endpoint URL\" specified above will be used to establish connection to the server instead of the discovered URL. " +
                    "Useful when connecting to OPC UA server through IP layer proxy or through SSH tunnel, in which the discovered URL is not reachable by the client.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> properties;

    private final String SECURITY_POLICY_PREFIX = "http://opcfoundation.org/UA/SecurityPolicy#";


    private OpcUaClient opcClient;
    private Map<String, UInteger> subscriptionMap;

    private final AtomicLong clientHandles = new AtomicLong(1L);

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT);
        props.add(SECURITY_POLICY);
        props.add(SECURITY_MODE);
        props.add(CLIENT_KS_LOCATION);
        props.add(CLIENT_KS_PASSWORD);
        props.add(REQUIRE_SERVER_AUTH);
        props.add(TRUSTSTORE_LOCATION);
        props.add(TRUSTSTORE_PASSWORD);
        props.add(AUTH_POLICY);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(USE_PROXY);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        String endpoint = context.getProperty(ENDPOINT).evaluateAttributeExpressions().getValue();
        if (endpoint == null) {
            throw new InitializationException("Endpoint can't be null.");
        }
        String securityPolicy = SECURITY_POLICY_PREFIX + context.getProperty(SECURITY_POLICY).getValue();

        // Get the Security Mode value: 1 - None; 2 - Sign; 3 - Sign and Encrypt
        int securityModeValue;
        if (context.getProperty(SECURITY_POLICY).getValue().equals("None")) {
            securityModeValue = 1;
        } else if (context.getProperty(SECURITY_MODE).getValue().equals("Sign")) {
            securityModeValue = 2;
        } else {
            securityModeValue = 3;
        }


        try {
            EndpointDescription[] endpoints =
                    UaTcpStackClient.getEndpoints(endpoint).get();

            EndpointDescription endpointDescription = null;
            for (EndpointDescription ed : endpoints) {

                if (ed.getSecurityPolicyUri().equals(securityPolicy)
                        && ed.getSecurityMode().getValue() == securityModeValue) {
                    endpointDescription = ed;
                    getLogger().debug("*** Connecting to endpoint " + ed.getEndpointUrl()
                            + " with security policy " + ed.getSecurityPolicyUri()
                            + " and security mode " + ed.getSecurityMode().name());
                }
            }

            if (endpointDescription == null) {
                throw new InitializationException("No endpoint with the specified security policy was found.");
            }

            OpcUaClientConfigBuilder cfgBuilder = new OpcUaClientConfigBuilder();

            // The following code is used to force the client to connect to the URL given by user,
            // instead of using the discovered URL. Useful when client is visiting the server through
            // some proxy or SSH tunneling, and the discovered URL is not reachable.
            if (context.getProperty(USE_PROXY).asBoolean()) {
                endpointDescription =  new EndpointDescription(endpoint,
                        endpointDescription.getServer(),
                        endpointDescription.getServerCertificate(),
                        endpointDescription.getSecurityMode(),
                        endpointDescription.getSecurityPolicyUri(),
                        endpointDescription.getUserIdentityTokens(),
                        endpointDescription.getTransportProfileUri(),
                        endpointDescription.getSecurityLevel());
            }

                cfgBuilder.setEndpoint(endpointDescription);
            if (!context.getProperty(SECURITY_POLICY).getValue().equals("None")) {  // If security policy is used

                // clientKsLocation has already been validated, no need to check again
                String clientKsLocation = context.getProperty(CLIENT_KS_LOCATION).evaluateAttributeExpressions().getValue();
                char[] clientKsPassword = context.getProperty(CLIENT_KS_PASSWORD).evaluateAttributeExpressions().getValue() != null ?
                        context.getProperty(CLIENT_KS_PASSWORD).evaluateAttributeExpressions().getValue().toCharArray() : null;

                // Verify server certificate against the trust store
                if (context.getProperty(REQUIRE_SERVER_AUTH).asBoolean()) {

                    // trustStoreLocation has already been validated, no need to check again
                    String trustStoreLocation = context.getProperty(TRUSTSTORE_LOCATION).evaluateAttributeExpressions().getValue();
                    char[] trustStorePassword = context.getProperty(TRUSTSTORE_PASSWORD).evaluateAttributeExpressions().getValue() != null ?
                            context.getProperty(TRUSTSTORE_PASSWORD).evaluateAttributeExpressions().getValue().toCharArray() : null;

                    TrustStoreLoader tsLoader = new TrustStoreLoader().load(trustStoreLocation, trustStorePassword);
                    List<X509Certificate> serverCerts = CertificateUtil.decodeCertificates(
                            endpointDescription.getServerCertificate().bytes());

                    try {
                        // Only verify the first certificate, and CA certificate at the end of the chain. Intermediate certificates are not verified
                        tsLoader.verify(serverCerts);
                    } catch (Exception e) {
                        getLogger().error("Cannot verify server certificate. Cause: " + e.getMessage()
                                + " Please make sure you have added the server certificate to the trust store.");
                        throw new InitializationException(e.getMessage());
                    }
                }

                KeyStoreLoader loader = new KeyStoreLoader().load(clientKsLocation, clientKsPassword);

                cfgBuilder.setCertificate(loader.getClientCertificate())
                        .setKeyPair(loader.getClientKeyPair())
                        .setRequestTimeout(uint(5000));
            }

            String authType = context.getProperty(AUTH_POLICY).getValue();
            IdentityProvider identityProvider;
            if (authType.equals("Anon")) {
                identityProvider = new AnonymousProvider();
            } else {
                String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
                username = username == null ? "" : username;
                String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
                identityProvider = new UsernameProvider(username == null ? "" : username,
                        password == null ? "" : password);
            }

            cfgBuilder.setIdentityProvider(identityProvider);


            opcClient = new OpcUaClient(cfgBuilder.build());
            opcClient.connect().get(5, TimeUnit.SECONDS);

            opcClient.getSubscriptionManager().addSubscriptionListener(new UaSubscriptionManager.SubscriptionListener() {
                @Override
                public void onSubscriptionTransferFailed(UaSubscription subscription, StatusCode statusCode) {
                    // TODO: this right now is just for logging purpose, need to further investigate the behavior for subscription transfer
                    getLogger().warn("Subscription transfer failed!");
                }

                @Override
                public void onPublishFailure(UaException exception) {
                    getLogger().warn("Subscription publish failure: " + exception.getMessage() + ", status code: " + exception.getStatusCode());
                }
            });

        } catch (Exception e) {
            throw new InitializationException(e);
        }


    }

    @OnDisabled
    public void shutdown() {
        try {
            if (opcClient != null) {
                getLogger().debug("Disconnecting from OPC server...");
                opcClient.disconnect().get(3, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            getLogger().warn(e.getMessage());
        }
    }


    @Override
    public byte[] getValue(List<String> tagNames, String returnTimestamp, boolean excludeNullValue,
                           String nullValueString) throws ProcessException {
        try {
            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            // TODO: Throw more descriptive exception when parsing fails
            ArrayList<NodeId> nodeIdList = new ArrayList<>();
            tagNames.forEach((tagName) -> nodeIdList.add(NodeId.parse(tagName)));


            List<DataValue> rvList = opcClient.readValues(0, TimestampsToReturn.Both, nodeIdList).get();

            StringBuilder serverResponse = new StringBuilder();

            for (int i = 0; i < tagNames.size(); i++) {
                String valueLine;
                try {
                    if (excludeNullValue && rvList.get(i).getValue().getValue().toString().equals(nullValueString)) {
                        getLogger().debug("Null value returned for " + rvList.get(i).getValue().getValue().toString()
                                + " -- Skipping because property is set");
                        continue;
                    }

                    valueLine = writeCsv(tagNames.get(i), returnTimestamp, rvList.get(i), excludeNullValue, nullValueString);

                } catch (Exception ex) {
                    getLogger().error("Error parsing result for " + tagNames.get(i));
                    valueLine = "";
                }

                serverResponse.append(valueLine);

            }

            return serverResponse.toString().trim().getBytes();

        } catch (Exception e) {
            throw new ProcessException(e);
        }

    }


    @Override
    public String subscribe(List<String> tagNames, BlockingQueue<String> queue, boolean tsChangedNotify) throws ProcessException {

        if (subscriptionMap == null) {
            subscriptionMap = new HashMap<>();
        }

        try {
            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            UaSubscription uaSubscription = opcClient.getSubscriptionManager().createSubscription(1000.0).get();

            // Create a list of MonitoredItemCreateRequest
            ArrayList<MonitoredItemCreateRequest> micrList = new ArrayList<>();
            tagNames.forEach((tagName) -> {

                ReadValueId readValueId = new ReadValueId(
                        NodeId.parse(tagName),
                        AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);

                Long clientHandleLong = clientHandles.getAndIncrement();
                UInteger clientHandle = uint(clientHandleLong);

                // Important!
                // If we apply this filter in MonitoringParameters, now not only we will get data when value changes,
                // we will also get data even value doesn't change, but the timestamp has changed.
                // If it is null, then the default DataChangeFilter will be used, which only get data when its value changes.
                DataChangeFilter df = tsChangedNotify ?
                        new DataChangeFilter(DataChangeTrigger.from(2), null, null) : null;

                MonitoringParameters parameters = new MonitoringParameters(
                        clientHandle,
                        300.0,     // sampling interval
                        ExtensionObject.encode(df),       // filter, null means use default
                        uint(10),   // queue size
                        true        // discard oldest
                );

                micrList.add(new MonitoredItemCreateRequest(
                        readValueId, MonitoringMode.Reporting, parameters));

            });

            // This is the callback when the MonitoredItem is created. In this callback, we set the consumer for incoming values
            BiConsumer<UaMonitoredItem, Integer> onItemCreated =
                    (item, id) -> item.setValueConsumer((it, value) -> {
                        getLogger().debug("subscription value received: item=" + it.getReadValueId().getNodeId()
                                + " value=" + value.getValue());
                        String valueLine = writeCsv(getFullName(it.getReadValueId().getNodeId()),
                                "Both", value, false, "");

                        queue.offer(valueLine);
                    });

            List<UaMonitoredItem> items = uaSubscription.createMonitoredItems(
                    TimestampsToReturn.Both,
                    micrList,
                    onItemCreated
            ).get();

            for (UaMonitoredItem item : items) {
                if (item.getStatusCode().isGood()) {
                    getLogger().debug("item created for nodeId=" + item.getReadValueId().getNodeId());
                } else {
                    getLogger().error("failed to create item for nodeId=" + item.getReadValueId().getNodeId()
                            + " (status=" + item.getStatusCode() + ")");
                }
            }

            String subscriptionUid;
            do {
                subscriptionUid = generateRandomChars(10);
            } while (subscriptionMap.containsKey(subscriptionUid));

            subscriptionMap.put(subscriptionUid, uaSubscription.getSubscriptionId());

            return subscriptionUid;

        } catch (Exception e) {
            throw new ProcessException(e.getMessage());
        }
    }

    @Override
    public void unsubscribe(String subscriptionUid) throws ProcessException {

        if (opcClient == null) {
            throw new ProcessException("OPC Client is null. OPC UA service was not enabled properly.");
        }

        if (subscriptionMap.get(subscriptionUid) != null) {

            try {

                opcClient.getSubscriptionManager()
                        .deleteSubscription(subscriptionMap.get(subscriptionUid)).get(4, TimeUnit.SECONDS);
                subscriptionMap.remove(subscriptionUid);
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }

    }

    @Override
    public byte[] getNodes(String indentString, int maxRecursiveDepth, int maxReferencePerNode,
                           boolean printNonLeafNode, String rootNodeId)
            throws ProcessException {

        try {
            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            NodeId nodeId;
            if (rootNodeId == null || rootNodeId.isEmpty()) {
                nodeId = Identifiers.RootFolder;
            } else {
                nodeId = NodeId.parse(rootNodeId);
            }

            StringBuilder builder = new StringBuilder();
            browseNodeIteratively("", indentString, maxRecursiveDepth, maxReferencePerNode, printNonLeafNode,
                    opcClient, nodeId, builder);

            return builder.toString().getBytes();

        } catch (Exception e) {
            throw new ProcessException(e.getMessage());
        }

    }

    // remainDepth = 0 means only print out the current node
    // StringBuilder is passed into the recursive method to reduce generating strings and improve performance
    private void browseNodeIteratively(String currentIndent, String indentString, int remainDepth, int maxRefPerNode,
                                       boolean printNonLeafNode, OpcUaClient client, NodeId browseRoot, StringBuilder builder) {

        //getLogger().info(indent + " Node=" + node.getNodeId().get().getIdentifier().toString());

        try {
            List<Node> nodes = client.getAddressSpace().browse(browseRoot).get();

            if (printNonLeafNode || nodes.size() == 0) {
                builder.append(currentIndent)
                        .append(getFullName(browseRoot))
                        .append("\n");
            }

            if (remainDepth > 0) {

                String newIndent = currentIndent + indentString;
                remainDepth--;

                int currNodeCount = 0;

                for (Node node : nodes) {
                    if (currNodeCount == maxRefPerNode)
                        break;

                    // recursively browse to children
                    browseNodeIteratively(newIndent, indentString, remainDepth, maxRefPerNode, printNonLeafNode,
                            client, node.getNodeId().get(), builder);

                    currNodeCount++;
                }
            }

        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Browsing nodeId=" + browseRoot + " failed: " + e.getMessage());
        }

    }

    private String getFullName(NodeId nodeId) {

        String identifierType;

        switch (nodeId.getType()) {
            case Numeric:
                identifierType = "i";
                break;
            case Opaque:
                identifierType = "b";
                break;
            case Guid:
                identifierType = "g";
                break;
            default:
                identifierType = "s";
        }

        return String.format("ns=%s;%s=%s", nodeId.getNamespaceIndex().toString(),
                identifierType, nodeId.getIdentifier().toString());
    }

    private String generateRandomChars(int length) {
        String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(candidateChars.charAt(random.nextInt(candidateChars
                    .length())));
        }
        return sb.toString();
    }

    private String writeCsv(String tagName, String returnTimestamp, DataValue value,
                            boolean excludeNullValue, String nullValueString) {

        String sValue = nullValueString;

        if(value == null || value.getValue() == null || value.getValue().getValue() == null) {

            if (excludeNullValue) {
                getLogger().debug("Null value returned for " + tagName
                        + " -- Skipping because property is set");
                return "";
            }

        } else {

            // Check the type of variant
            if(value.getValue().getValue().getClass().isArray()) {

                StringBuilder sb = new StringBuilder();
                Object[] arr = (Object[]) value.getValue().getValue();
                for (Object o : arr) {
                    sb.append(o.toString()).append(";");
                }
                sValue = sb.toString();

            } else {
                sValue = value.getValue().getValue().toString();
            }

        }

        StringBuilder valueLine = new StringBuilder();

        valueLine.append(tagName).append(",");

        if (("ServerTimestamp").equals(returnTimestamp) || ("Both").equals(returnTimestamp)) {
            if(value.getServerTime() != null) valueLine.append(value.getServerTime().getJavaTime());
            valueLine.append(",");
        }
        if (("SourceTimestamp").equals(returnTimestamp) || ("Both").equals(returnTimestamp)) {
            if(value.getSourceTime() != null) valueLine.append(value.getSourceTime().getJavaTime());
            valueLine.append(",");
        }

        valueLine.append(sValue);
        valueLine.append(",");

        valueLine.append(value.getStatusCode().getValue()).
                append(System.getProperty("line.separator"));

        return valueLine.toString();
    }
}
