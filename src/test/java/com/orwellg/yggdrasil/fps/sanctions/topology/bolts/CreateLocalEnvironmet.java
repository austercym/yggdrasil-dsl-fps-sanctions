package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.FPSEvents;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
import com.orwellg.yggdrasil.fps.sanctions.messages.FpsInboundMessage;

import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.producer.*;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


// We ignore "TEST" because this is just to create a local environment for testing purpose.
@Ignore
public class CreateLocalEnvironmet {

    protected Session ses;


    @org.junit.Test
    public void keyspace() {
        String partyId = "1";

        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");

        try {
            String selectQueryKeyspace = "CREATE KEYSPACE ipagoo WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };";

            PreparedStatement stKeyspace = ses.prepare(selectQueryKeyspace);
            BoundStatement selectKeyspace = stKeyspace.bind();
            ses.execute(selectKeyspace);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @org.junit.Test
    public void keyspaceDefault() {
        String partyId = "1";

        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");

        try {
            String selectQueryKeyspace = "CREATE KEYSPACE customer_product_db WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };";

            PreparedStatement stKeyspace = ses.prepare(selectQueryKeyspace);
            BoundStatement selectKeyspace = stKeyspace.bind();
            ses.execute(selectKeyspace);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @org.junit.Test
    public void shortAccount() {


        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");


        try {

            String selectQueryTable2 = "DROP TABLE IF EXISTS ipagoo.SortCodeAccount;";
            PreparedStatement stTable2= ses.prepare(selectQueryTable2);
            BoundStatement selectTable2 = stTable2.bind();
            ses.execute(selectTable2);

            String selectQueryTable = "CREATE TABLE ipagoo.SortCodeAccount ( SortCodeAccount varchar, InternalAccountId bigint, PRIMARY KEY (SortCodeAccount));";
            PreparedStatement stTable= ses.prepare(selectQueryTable);
            BoundStatement selectTable = stTable.bind();
            ses.execute(selectTable);

            String insertQueryTable = "INSERT INTO ipagoo.SortCodeAccount (SortCodeAccount,InternalAccountId ) VALUES ('233456', '65000300000');";
            PreparedStatement stTable3= ses.prepare(insertQueryTable);
            BoundStatement selectTable3 = stTable3.bind();
            ses.execute(selectTable3);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    @org.junit.Test
    public void ibanAccount() {
        String partyId = "1";

        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");


        try {

            String selectQueryTable2 = "DROP TABLE IF EXISTS ipagoo.IBANAccount;";
            PreparedStatement stTable2= ses.prepare(selectQueryTable2);
            BoundStatement selectTable2 = stTable2.bind();
            ses.execute(selectTable2);

            String selectQueryTable = "CREATE TABLE ipagoo.IBANAccount (IBAN varchar,InternalAccountId varchar, PRIMARY KEY (IBAN));";
            PreparedStatement stTable= ses.prepare(selectQueryTable);
            BoundStatement selectTable = stTable.bind();
            ses.execute(selectTable);

            String insertQueryTable = "INSERT INTO ipagoo.IBANAccount (IBAN,InternalAccountId ) VALUES ('BE30001216371411', '65000300000');";
            PreparedStatement stTable3= ses.prepare(insertQueryTable);
            BoundStatement selectTable3 = stTable3.bind();
            ses.execute(selectTable3);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println("aaaaa");
    }

    @org.junit.Test
    public void Product() {
        String partyId = "1";

        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");


        try {

            String selectQueryTable2 = "DROP TABLE IF EXISTS ipagoo.Products;";
            PreparedStatement stTable2= ses.prepare(selectQueryTable2);
            BoundStatement selectTable2 = stTable2.bind();
            ses.execute(selectTable2);

            String selectQueryTable = "CREATE TABLE ipagoo.Products (ProductId varchar,PartyId varchar,BrandProductTypeId varchar,ProductTypeId varchar,SubProductTypeId varchar,PPHR varchar,ContractId varchar,Status varchar,MLE varchar,SLE varchar,ProductFamily varchar,ProductFeatures text,ProductOperations text,ProductAddress text,PRIMARY KEY (ProductId));";
            PreparedStatement stTable= ses.prepare(selectQueryTable);
            BoundStatement selectTable = stTable.bind();
            ses.execute(selectTable);

            String insertQueryTable = "INSERT INTO ipagoo.Products (ProductId,PartyId,BrandProductTypeId,ProductTypeId,SubProductTypeId,PPHR,ContractId,Status,MLE,SLE,ProductFamily,ProductFeatures,ProductOperations,ProductAddress) VALUES ('65000300000', '', '', '', '', '', '', 'active', '', '', '', '', '', '');";
            PreparedStatement stTable3= ses.prepare(insertQueryTable);
            BoundStatement selectTable3 = stTable3.bind();
            ses.execute(selectTable3);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println("aaaaa");
    }

    @org.junit.Test
    public void FPSPaymentRequest() {
        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");


        try {

            String selectQueryTable2 = "DROP TABLE IF EXISTS ipagoo.FPSPaymentRequest;";
            PreparedStatement stTable2= ses.prepare(selectQueryTable2);
            BoundStatement selectTable2 = stTable2.bind();
            ses.execute(selectTable2);

            String selectQueryTable = "CREATE TABLE ipagoo.FPSPaymentRequest (PaymentId varchar, FPID varchar, CreDtTm timestamp, IntrBkSttlmAmt decimal, PRIMARY KEY(PaymentId));";
            PreparedStatement stTable= ses.prepare(selectQueryTable);
            BoundStatement selectTable = stTable.bind();
            ses.execute(selectTable);

            String insertQueryTable = "INSERT INTO ipagoo.FPSPaymentRequest (PaymentId, FPID, CreDtTm, IntrBkSttlmAmt)  VALUES ('FPS1711150000002','FPS1711150000002  1020171115826400000     ', 1382655211694, 40.230000);";
            PreparedStatement stTable3= ses.prepare(insertQueryTable);
            BoundStatement selectTable3 = stTable3.bind();
            ses.execute(selectTable3);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @org.junit.Test
    public void EiscdFPS() {
        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");


        try {

            String selectQueryTable2 = "DROP TABLE IF EXISTS ipagoo.eiscd_fasterpayment;";
            PreparedStatement stTable2= ses.prepare(selectQueryTable2);
            BoundStatement selectTable2 = stTable2.bind();
            ses.execute(selectTable2);

            String selectQueryTable = "CREATE TABLE ipagoo.eiscd_fasterpayment (sortcode varchar, status varchar,fpslastchangedate timestamp,fpscloseddate timestamp,redirection varchar,redirectsortcode varchar, settlebank_conncode varchar, settlebank_bankcode varchar, handlingbank_conncode varchar, handlingbank_bankcode varchar, acctnmflag varchar, agencytype varchar, lastchangedate timestamp, printind varchar, PRIMARY KEY (sortcode),);";
            PreparedStatement stTable= ses.prepare(selectQueryTable);
            BoundStatement selectTable = stTable.bind();
            ses.execute(selectTable);

            String insertQueryTable = "INSERT INTO ipagoo.eiscd_fasterpayment (sortcode, status,fpslastchangedate,fpscloseddate,redirection,redirectsortcode, settlebank_conncode, settlebank_bankcode, handlingbank_conncode, handlingbank_bankcode, acctnmflag, agencytype, lastchangedate, printind) VALUES ('233456','blocked',1382655211694,'','','','','','','','','',1382655211694,'');";
            PreparedStatement stTable3= ses.prepare(insertQueryTable);
            BoundStatement selectTable3 = stTable3.bind();
            ses.execute(selectTable3);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @org.junit.Test
    public void Accounting() {
        String clusterConfiguration = "localhost:9042";
        ScyllaManager man = ScyllaManager.getInstance(clusterConfiguration);
        ses = man.getSession("system");


        try {

            String selectQueryTable2 = "DROP TABLE IF EXISTS ipagoo.AccountTransactionLog;";
            PreparedStatement stTable2= ses.prepare(selectQueryTable2);
            BoundStatement selectTable2 = stTable2.bind();
            ses.execute(selectTable2);

            String selectQueryTable = "CREATE TABLE ipagoo.AccountTransactionLog ( AccountId varchar,ActualBalance decimal,LedgerBalance decimal,LastTransactionId varchar,TransactionLogId varchar,TransactionStoreAction varchar,TransactionType varchar,TransactionCode varchar,TransactionDescription varchar,TransactionTimestamp timestamp,TransactionTimenanos varchar,TransactionTimezone varchar,TransactionAmount decimal,TransactionCurrency varchar,ExchangeRate decimal,ChequeNumber varchar,TellerId varchar,CashFlag varchar,PRIMARY KEY (AccountId, TransactionTimestamp, TransactionLogId));";
            PreparedStatement stTable= ses.prepare(selectQueryTable);
            BoundStatement selectTable = stTable.bind();
            ses.execute(selectTable);

            String insertQueryTable = "INSERT INTO ipagoo.AccountTransactionLog (AccountId,ActualBalance,LedgerBalance,LastTransactionId,TransactionLogId,TransactionStoreAction,TransactionType,TransactionCode ,TransactionDescription ,TransactionTimestamp ,TransactionTimenanos ,TransactionTimezone ,TransactionAmount ,TransactionCurrency ,ExchangeRate ,ChequeNumber ,TellerId ,CashFlag)  VALUES ('650100IPAGO',10,10,'IPAGO','TRA','STOR','TYPE','00','DES',1382655211694,'1234','',0,'',0,'','','');";
            PreparedStatement stTable3= ses.prepare(insertQueryTable);
            BoundStatement selectTable3 = stTable3.bind();
            ses.execute(selectTable3);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @org.junit.Test
    public void zookeeper() throws Exception {

        CuratorFramework client;

        String zookeeperHost = "localhost:2181";
        client = CuratorFrameworkFactory.newClient(zookeeperHost, new ExponentialBackoffRetry(1000, 3));
        client.start();

        ZooKeeperHelper zk = new ZooKeeperHelper(client);

        zk.printAllProps();

        zk.setZkProp("/com/orwellg/yggdrasil/network", "Path_for_Network_Configuration" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.node.list", "1,2,3");
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.accounting.min.value", "650001" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.accounting.max.value", "650152" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yyggdrassil.network.node.name.prefix", "com.orwellg.accounting.processor" );

        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.1.node.host", "10.1.0.30" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.1.node.port", "9079" );

        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.1.node.operation.order", "DEBIT_CREDIT" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.1.fps.account", "990010" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.1.gps.account", "990020" );

        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.2.node.host", "10.1.0.29" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.2.node.port", "9079" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.2.node.operation.order", "DEBIT_CREDIT" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.2.fps.account", "990011" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.2.gps.account", "990021" );

        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.3.node.host", "10.1.0.31" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.3.node.port", "9079" );

        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.3.node.operation.order", "DEBIT_CREDIT" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.3.fps.account", "990012" );
        zk.setZkProp("/com/orwellg/yggdrasil/network/yggdrassil.network.3.gps.account", "990022" );

         // scylla
        String scyllaNodes = "localhost:9042";
        String scyllaKeyspace = "ipagoo";
        zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list", scyllaNodes);
        zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.host.list", "localhost");
        zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace.customer.product", scyllaKeyspace);
        zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace.reference.data", "reference_db");



        zk.printAllProps();
    }

    @org.junit.Test
//    public void producer() throws IOException {
//
//        TestCallback callback = new TestCallback();
//
//        //String bootstrapServer = TopologyConfigFactory.getTopologyConfig().getKafkaBootstrapHosts();
//
//        Properties propsP = new Properties();
//        propsP.put("bootstrap.servers", "localhost:9092");
//        propsP.put(ProducerConfig.ACKS_CONFIG, "all");
//        propsP.put(ProducerConfig.RETRIES_CONFIG, 0);
//        propsP.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        propsP.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer<String, String> producer = new KafkaProducer<String, String>(propsP);
//
//        URL url = Resources.getResource("pacs_008_example.json");
//        String text = Resources.toString(url, Charsets.UTF_8);
//
//        Gson gson = new Gson();
//
//        FpsInboundMessage inboundMessage = gson.fromJson(text,FpsInboundMessage.class);
//
//        String eventName = FPSEvents.FPS_INBOUND_REQUEST_RECEIVED.getEventName();
//        Event event = generateRequestEvents("CreateLocalEnvironmet", eventName, inboundMessage);
//
//        String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
//        ProducerRecord record = new ProducerRecord<String, String>("com.orwellg.yggdrasil.fps.inbound.request",  UUID.randomUUID().toString(),base64Event);
//        producer.send(record,callback);
//
//        producer.close();
//
//    }

    protected Event generateRequestEvents(String parentKey, String eventName, FpsInboundMessage element) {
        // Generate Events with eventData
        List<Event> events = new ArrayList<>();

            FpsInboundMessage t = element;
            String serializedType = new Gson().toJson(t);
            String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
            String uuid = UUID.randomUUID().toString();
            String eventKeyId = "EVENT-" + uuid;

            Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedType);
            events.add(event);


        return event;
    }

    protected Event generateEvent(String parentKey, String processId, String eventKeyId, String eventName, String serializedData) {

        String logPreffix = String.format("[Key: %s][ProcessId: %s]: ", parentKey, processId);


        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion());
        eventType.setParentKey(parentKey);
        eventType.setKey(eventKeyId);
        eventType.setSource(this.getClass().getSimpleName());
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));

        eventType.setData(serializedData);

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid(processId);

        EntityIdentifierType entityIdentifier = new EntityIdentifierType();
        entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
        entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

        // Create the corresponden event
        Event event = new Event();
        event.setEvent(eventType);
        event.setProcessIdentifier(processIdentifier);
        event.setEntityIdentifier(entityIdentifier);


        return event;
    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + e.getMessage());
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

//    @org.junit.Test
//    public void testProducer() throws IOException {
//
//        URL url = Resources.getResource("pacs_008_return_example.json");
//        String text = Resources.toString(url, Charsets.UTF_8);
//
//        Gson gson = new Gson();
//
//        FpsInboundMessage inboundMessage = gson.fromJson(text,FpsInboundMessage.class);
//
//        String eventName = FPSEvents.FPS_INBOUND_REQUEST_RECEIVED.getEventName();
//        Event event = generateRequestEvents("CreateLocalEnvironmet", eventName, inboundMessage);
//
//        String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
//
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
//        props.put(ProducerConfig.RETRIES_CONFIG, 0);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//        TestCallback callback = new TestCallback();
//
//        ProducerRecord<String, String> data = new ProducerRecord<String, String>("com.orwellg.yggdrasil.dsl.fps.inbound.payment.request.1", UUID.randomUUID().toString(), base64Event);
//        producer.send(data, callback);
//
//        producer.close();
//
//    }


}
