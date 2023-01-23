package org.iviliev.sfQueueStuck;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.management.impl.view.QueueField;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPubSubGettingStuck {
    private static final Logger log = LoggerFactory.getLogger(TestPubSubGettingStuck.class);

    private static final Set<QueueField> QUEUE_DATA = EnumSet.of(
            QueueField.ID,
            QueueField.NAME,
            QueueField.ADDRESS,
            QueueField.CONSUMER_COUNT,
            QueueField.MESSAGES_ADDED,
            QueueField.MESSAGE_COUNT,
            QueueField.MESSAGES_ACKED,
            QueueField.MESSAGES_EXPIRED,
            QueueField.DELIVERING_COUNT,
            QueueField.MESSAGES_KILLED
    );
    private static final ScheduledExecutorService e = Executors.newScheduledThreadPool(15);

    @Test
    public void test() throws Exception {
        ActiveMQServer b1 = createBroker(jmsBrokerConfiguration("broker1", "127.0.0.1", "5000"));
        ActiveMQServer b2 = createBroker(jmsBrokerConfiguration("broker2", "127.0.0.1", "6000"));

        ActiveMQConnectionFactory b1cf = getConnectionFactory("vm://5000");
        ActiveMQConnectionFactory b2cf = getConnectionFactory("vm://6000");

        AtomicLong slowSent = new AtomicLong();
        AtomicLong fastestSent = new AtomicLong();
        AtomicLong fasterSent = new AtomicLong();

        AtomicLong slowReceived = new AtomicLong();
        AtomicLong fastestReceived = new AtomicLong();
        AtomicLong fasterReceived = new AtomicLong();

        pubSub("SLOW_1", b1cf, b2cf, 512 * 1024, slowSent, m -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                slowReceived.incrementAndGet();
            } catch (InterruptedException ex) {
                log.error("Interrupted slow consumer");
            }
        });
        pubSub(
                "FASTEST_1",
                b1cf,
                b2cf,
                32 * 1024,
                fastestSent,
                m -> fastestReceived.incrementAndGet()
        );
        pubSub(
                "FASTER_1",
                b1cf,
                b2cf,
                64 * 1024,
                fasterSent,
                m -> fasterReceived.incrementAndGet()
        );

        e.scheduleAtFixedRate(() -> {
            List<Map<String, String>> myQueues1 = null;
            List<Map<String, String>> myQueues2 = null;
            try {
                myQueues1 = getMyQueues(b1);
                myQueues2 = getMyQueues(b2);
            } catch (Exception ex) {
                log.error("Could not list queues", ex);
            }

            log.info("\n" +
                     myQueues1 + "\n" +
                     myQueues2 + "\n" +
                     "Received/Sent: [SLOW_1: {}/{}, FASTEST_1: {}/{}, FASTER_1: {}/{}]",
                    slowReceived, slowSent, fastestReceived, fastestSent, fasterReceived, fasterSent
            );
        }, 0, 5, TimeUnit.SECONDS);

        new CountDownLatch(1).await();
    }

    private void pubSub(String topic, ActiveMQConnectionFactory from, ActiveMQConnectionFactory to,
                        int msgSize, AtomicLong sentCount, MessageListener onReceive)
            throws JMSException {
        Connection c2 = to.createConnection();
        c2.start();
        Session s2 = c2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer mp2 = s2.createConsumer(s2.createTopic(topic));
        mp2.setMessageListener(onReceive);

        Connection c1 = from.createConnection();
        c1.start();
        Session s1 = c1.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final MessageProducer mp1 = s1.createProducer(s1.createTopic(topic));
        mp1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        e.scheduleWithFixedDelay(() -> {
            try {
                mp1.send(s1.createObjectMessage(RandomUtils.nextBytes(msgSize)));
                sentCount.incrementAndGet();
            } catch (Exception ex) {
                log.error("Could not send", ex);
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }

    private List<Map<String, String>> getMyQueues(ActiveMQServer b) throws Exception {
        Map<String, Object> filter = Map.of(
                "field", "autoCreated",
                "operation", "CONTAINS",
                "value", "true"
        );
        String queues = b.getActiveMQServerControl().listQueues(JsonUtil.toJsonObject(filter)
                .toString(), 1, 100);
        return JsonUtil
                .readJsonObject(queues)
                .getJsonArray("data")
                .stream()
                .map(q -> q
                        .asJsonObject()
                        .entrySet()
                        .stream()
                        .filter(e -> QUEUE_DATA.contains(QueueField.valueOfName(e.getKey())))
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())))
                .collect(Collectors.toList());
    }

    private void addAcceptorsAndConnectors(
            org.apache.activemq.artemis.core.config.Configuration configuration, String cellIp,
            String jmsPort) throws Exception {
        configuration.addAcceptorConfiguration("invm-acceptor", "vm://" + jmsPort);
        configuration.addAcceptorConfiguration("netty-acceptor", "tcp://" + cellIp + ":" + jmsPort);
        configuration.addConnectorConfiguration(
                "netty-connector",
                "tcp://" + cellIp + ":" + jmsPort
        );
    }

    private void addClusterConfigurationHelper(
            org.apache.activemq.artemis.core.config.Configuration configuration) {

        final String bgName = "broadcast-group";
        final String dgName = "discovery-group";

        final List<String> connectorPairs = Collections.singletonList("netty-connector");

        UDPBroadcastEndpointFactory bee = new UDPBroadcastEndpointFactory()
                .setGroupAddress("231.7.7.7")
                .setGroupPort(9876);

        final BroadcastGroupConfiguration bgConfig = new BroadcastGroupConfiguration()
                .setName(bgName)
                .setBroadcastPeriod(500)
                .setConnectorInfos(connectorPairs)
                .setEndpointFactory(bee);

        final DiscoveryGroupConfiguration dgConfig = new DiscoveryGroupConfiguration()
                .setName(dgName)
                .setRefreshTimeout(10000)
                .setDiscoveryInitialWaitTimeout(1000)
                .setBroadcastEndpointFactory(bee);

        final ClusterConnectionConfiguration clusterConf = new ClusterConnectionConfiguration()
                .setName("my-cluster")
                .setConnectorName("netty-connector")
                .setClientFailureCheckPeriod(30000)
                .setConnectionTTL(60000)
                .setRetryInterval(2000)
                .setMaxRetryInterval(20000)
                //                .setRetryIntervalMultiplier(2)
                .setReconnectAttempts(-1)
                .setInitialConnectAttempts(-1)
                .setDuplicateDetection(true)
                .setAllowDirectConnectionsOnly(false)
                .setMessageLoadBalancingType(MessageLoadBalancingType.ON_DEMAND)
                .setMaxHops(1)
                .setDiscoveryGroupName(dgName);

        configuration
                .addBroadcastGroupConfiguration(bgConfig)
                .addDiscoveryGroupConfiguration(dgName, dgConfig)
                .addClusterConfiguration(clusterConf)
                .addAddressesSetting("#", getAddressSettings())
                .addAddressesSetting("SLOW*", getSlowAddressSettings())
                .setWildCardConfiguration(getWildCardConfiguration());

        configuration.addAddressConfiguration(
                new CoreAddressConfiguration()
                        .setName("DLQ")
                        .addRoutingType(RoutingType.ANYCAST)
                        .addQueueConfiguration(new QueueConfiguration("DLQ").setRoutingType(
                                RoutingType.ANYCAST)));
        configuration.addAddressConfiguration(
                new CoreAddressConfiguration()
                        .setName("ExpiryQueue")
                        .addRoutingType(RoutingType.ANYCAST)
                        .addQueueConfiguration(new QueueConfiguration("ExpiryQueue").setRoutingType(
                                RoutingType.ANYCAST)));
    }

    private ActiveMQServer createBroker(
            org.apache.activemq.artemis.core.config.Configuration jmsBrokerConfiguration)
            throws Exception {

        final ActiveMQServer broker = new ActiveMQServerImpl(
                jmsBrokerConfiguration,
                null,
                new ActiveMQSecurityManager() {

                    @Override
                    public boolean validateUser(String user, String password) {
                        return true;
                    }

                    @Override
                    public boolean validateUserAndRole(String user, String password,
                                                       Set<Role> roles,
                                                       CheckType checkType) {
                        return true;
                    }
                }
        ) {
            @Override
            protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
                NodeManager nm = new InVMNodeManager(replicatingBackup);
                nm.setNodeID(UUID.nameUUIDFromBytes(jmsBrokerConfiguration.getName().getBytes())
                        .toString());
                return nm;
            }
        };

        broker.start();
        broker.waitForActivation(15, TimeUnit.SECONDS);

        broker
                .getClusterManager()
                .getClusterController()
                .getDefaultClusterTopology()
                .addClusterTopologyListener(new ClusterTopologyListener() {

                    @Override
                    public void nodeDown(long eventUID, String nodeID) {
                        log.info(
                                "{} nodeDown {} {}",
                                jmsBrokerConfiguration.getName(),
                                nodeID,
                                new Date()
                        );
                    }

                    @Override
                    public void nodeUP(TopologyMember member, boolean last) {
                        log.info(
                                "{} nodeUp {} {}",
                                jmsBrokerConfiguration.getName(),
                                member,
                                new Date()
                        );
                    }
                });

        return broker;
    }

    private AddressSettings getAddressSettings() {
        final long brokerMemPerQueue = 6 * 1024 * 1024;
        final AddressSettings addressSettings = new AddressSettings();
        addressSettings.setRedistributionDelay(0);
        addressSettings.setDefaultAddressRoutingType(RoutingType.MULTICAST);
        addressSettings.setAutoCreateAddresses(true);
        addressSettings.setAutoDeleteAddresses(false);
        addressSettings.setAutoCreateQueues(true);
        addressSettings.setAutoDeleteQueues(false);
        addressSettings.setDeadLetterAddress(SimpleString.toSimpleString("DLQ"));
        addressSettings.setExpiryDelay(7 * 60 * 1000L);
        addressSettings.setMaxSizeBytes(brokerMemPerQueue);
        addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
        return addressSettings;
    }

    private AddressSettings getSlowAddressSettings() {
        final long brokerMemPerQueue = 16 * 1024 * 1024;

        final AddressSettings addressSettings = new AddressSettings();
        addressSettings.setRedistributionDelay(-1);
        addressSettings.setDefaultAddressRoutingType(RoutingType.MULTICAST);
        addressSettings.setAutoCreateAddresses(true);
        addressSettings.setAutoDeleteAddresses(true);
        addressSettings.setAutoCreateQueues(true);
        addressSettings.setAutoDeleteQueues(true);
        addressSettings.setDeadLetterAddress(SimpleString.toSimpleString("DLQ"));
        addressSettings.setExpiryDelay(30000L);
        addressSettings.setMaxSizeBytes(brokerMemPerQueue);
        addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
        return addressSettings;
    }

    private ActiveMQConnectionFactory getConnectionFactory(String url) {
        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
        cf.setUser("brokerSystemUser");
        cf.setPassword("password");
        cf.setDeserializationWhiteList("*");
        cf.setBlockOnAcknowledge(false);
        cf.setBlockOnDurableSend(false);
        cf.setBlockOnNonDurableSend(false);

        return cf;
    }

    private WildcardConfiguration getWildCardConfiguration() {
        final WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
        wildcardConfiguration.setSingleWord('*');
        wildcardConfiguration.setAnyWords('#');
        wildcardConfiguration.setDelimiter('/');
        wildcardConfiguration.setRoutingEnabled(true);
        return wildcardConfiguration;
    }

    private org.apache.activemq.artemis.core.config.Configuration jmsBrokerConfiguration(
            String jmsBrokerName, String host, String jmsPort) throws Exception {
        final String brokerRootDir = Files
                .createTempDirectory(Paths.get("target/"), jmsBrokerName)
                .toAbsolutePath()
                .toString();
        final long brokerMemMaxSize = 64 * 1024 * 1024;

        final ConfigurationImpl configuration = new ConfigurationImpl();
        configuration
                .setName(jmsBrokerName)
                .setPersistenceEnabled(true)
                .setSecurityEnabled(true)
                .setJournalMinFiles(2)
                .setJournalFileSize(10 * 1024 * 1024)
                .setJournalPoolFiles(10)
                .setMaxDiskUsage(100)
                .setJournalType(JournalType.NIO)
                .setJournalDirectory(brokerRootDir)
                .setBindingsDirectory(brokerRootDir)
                .setPagingDirectory(brokerRootDir)
                .setLargeMessagesDirectory(brokerRootDir)
                .setJournalCompactMinFiles(10)
                .setJournalCompactPercentage(30)
                .setClusterUser("brokerClusterUser")
                .setClusterPassword("password")
                .setJournalDatasync(false)
                .setJMXManagementEnabled(true)
                .setGlobalMaxSize(brokerMemMaxSize);

        configuration.setCriticalAnalyzer(true);
        configuration.setCriticalAnalyzerPolicy(CriticalAnalyzerPolicy.LOG);
        configuration.setJournalSyncNonTransactional(false);

        configuration.setNodeManagerLockDirectory(brokerRootDir);

        addAcceptorsAndConnectors(configuration, host, jmsPort);

        addClusterConfigurationHelper(configuration);

        return configuration;
    }
}
