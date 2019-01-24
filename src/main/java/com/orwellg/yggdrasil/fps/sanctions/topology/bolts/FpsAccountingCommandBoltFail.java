package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.command.accounting.AccountInfo;
import com.orwellg.umbrella.avro.types.command.accounting.AccountingCommandData;
import com.orwellg.umbrella.avro.types.command.accounting.AccountingInfo;
import com.orwellg.umbrella.avro.types.command.accounting.BalanceUpdateType;
import com.orwellg.umbrella.avro.types.command.accounting.TransactionAccountingInfo;
import com.orwellg.umbrella.avro.types.command.accounting.TransactionDirection;
import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.commons.KeyValue;
import com.orwellg.umbrella.avro.types.commons.TransactionType;
import com.orwellg.umbrella.avro.types.payment.fps.FPSSanctionsAction;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.utils.enums.AccountingTagsTypes;
import com.orwellg.umbrella.commons.utils.enums.CommandTypes;
import com.orwellg.umbrella.commons.utils.enums.KafkaHeaders;
import com.orwellg.umbrella.commons.utils.enums.Systems;
import com.orwellg.umbrella.commons.utils.enums.TransactionEvents;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.commons.utils.constants.Constants;
import com.orwellg.yggdrasil.commons.utils.enums.SpecialAccountTypes;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfig;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfigFactory;
import com.orwellg.yggdrasil.fps.sanctions.scylla.entities.FpsPaymentRequest;


public class FpsAccountingCommandBoltFail extends BasicRichBolt {
	private static final long serialVersionUID = 1L;


	public final static Logger LOG = LogManager.getLogger(FpsAccountingCommandBoltFail.class);

	private String zookeeperHost;
    private Cluster processorCluster;

    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	super.prepare(stormConf, context, collector);
    	zookeeperHost = (String) stormConf.get(Constants.ZK_HOST_LIST_PROPERTY); 
    	processorCluster = ClusterFactory.createCluster(FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig(zookeeperHost).getNetworkConfig());
    }
    
    
    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "result", "commandName", "eventName", "eventData", "topic", "sanctionAction"}));

    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Getting ready for executing AccountingCommand Bolt. Tuple values: {}", tuple);
        FpsSanctionsTopologyConfig topologyConfig = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig(zookeeperHost);
        FpsPaymentRequest data = (FpsPaymentRequest) tuple.getValueByField("eventData");
        FPSSanctionsAction sanctionAction = (FPSSanctionsAction) tuple.getValueByField("sanctionAction");
        FpsPaymentRequest fpsPaymentRequest = (FpsPaymentRequest) tuple.getValueByField("fpsPaymentRequest");
        String eventKey = tuple.getStringByField("key");
        String pmtId = tuple.getStringByField("processId");
        
        try {
            LOG.info("[PmtId: {}] Received sanction with event key {} to make accounting on account {}", pmtId, eventKey, fpsPaymentRequest.getCdtrAccountId());
            Map<String, Object> values = new HashMap<>();

            if (processorCluster == null) {
            	throw new Exception("Error obtaining configuration data for Accounting Processor Cluster. Processor Clustes is null");
            }
            
            processorCluster.getNodes().forEach( (numNode, node) -> {
                LOG.info("[Processor Cluster] Num Node: {} - Range: [{}-{}]", numNode, node.getInitialAccountRange(), node.getFinalAccountRange());
            });

            Node processorNode = getProcessorNode(fpsPaymentRequest.getCdtrAccountId());

            String debitorAccountId = processorNode.getSpecialAccount(SpecialAccountTypes.SANCTIONS.getLiteral());
            String creditorAccountId = processorNode.getSpecialAccount(SpecialAccountTypes.FPS.getLiteral());

            LOG.info("[PmtId: {}] FPS Sanctions Account: {} ", pmtId, debitorAccountId);

            values.put("topic", processorNode.getTopic());
            values.put("key", pmtId);
            values.put("processId", pmtId);
            values.put("eventName", TransactionEvents.FPS_POO_PAYMENT_SANCTION_FAILED.getEventName());
            values.put("headers", addHeaderValue(tuple.getStringByField("headers"), KafkaHeaders.REPLY_TO.getKafkaHeader(), topologyConfig.getTopologyPropertyValue(FpsSanctionsTopologyConfig.PROPERTY_TOPIC_ACCTNG_RESPONSE).getBytes()));
            values.put("commandName", CommandTypes.ACCOUNTING_COMMAND_RECEIVED.getCommandName());
            values.put("eventData", data);

            List<KeyValue> listTags = getListTags(data,sanctionAction);
            values.put("result", generateCommand(data, sanctionAction, debitorAccountId,creditorAccountId,pmtId,listTags));

            LOG.info("[PmtId: {}] Sending to Accounting Command to topic: {} ", pmtId, processorNode.getTopic());
            LOG.info("Sending tuple values. Tuple: {}", values);
            send(tuple, values);
            
        } catch (Exception ex) {
	        	LOG.error("[PmtId: {}] Error preparing accounting command for inbound payment on account {}. Error message: {}", pmtId, fpsPaymentRequest.getCdtrAccountId(), ex.getMessage(), ex);
	        	error(ex, tuple);
        }

    }


    private AccountingCommandData generateCommand(FpsPaymentRequest fpsPaymentRequest, FPSSanctionsAction fpsSanctionAction, String debitAccount, String creditAccount, String key,List<KeyValue> listTags) {

        AccountingCommandData commandData = new AccountingCommandData();
        commandData.setAccountingInfo(new AccountingInfo());

        AccountInfo debitAccountInfo = new AccountInfo();
        debitAccountInfo.setAccountId(debitAccount);
        AccountInfo creditAccountInfo = new AccountInfo();
        creditAccountInfo.setAccountId(creditAccount);

        commandData.getAccountingInfo().setDebitAccount(debitAccountInfo);
        commandData.getAccountingInfo().setCreditAccount(creditAccountInfo);
        commandData.getAccountingInfo().setDebitBalanceUpdate(BalanceUpdateType.ALL);
        commandData.getAccountingInfo().setCreditBalanceUpdate(BalanceUpdateType.ALL);
        Decimal amountDec = new Decimal();
        amountDec.setValue(fpsPaymentRequest.getTransactionAmount());
        String currency = fpsPaymentRequest.getCurrency();

        commandData.setTransactionInfo(new TransactionAccountingInfo());
        commandData.getTransactionInfo().setAmount(amountDec);
        commandData.getTransactionInfo().setCurrency(currency);
        commandData.getTransactionInfo().setData(new Gson().toJson(fpsPaymentRequest.getRequestTimeStamp()));
        commandData.getTransactionInfo().setId(key);
        commandData.getTransactionInfo().setSystem(fpsSanctionAction.getPaymentSystem());
        commandData.getTransactionInfo().setDirection(TransactionDirection.INCOMING);
        commandData.getTransactionInfo().setTransactionType(TransactionType.CREDIT);

        // Entry Origin
        commandData.setEntryOrigin(Systems.FPS.getSystem());
        commandData.setAccountingTags(listTags);

        return commandData;
    }

    private List<KeyValue> getListTags(FpsPaymentRequest fpsPaymentRequest, FPSSanctionsAction fpsSanctionAction){
    		Gson gson = new Gson();
        // Including TAGS for Faster Payment
        List<KeyValue> listTags = new ArrayList<>();
        // PaymentType
        listTags.add(new KeyValue(AccountingTagsTypes.FPS_PAYMENT_TYPE.getTag(), fpsPaymentRequest.getPaymentType()));
        // isPOO
        listTags.add(new KeyValue(AccountingTagsTypes.FPS_PAYMENT_POO.getTag(), "true"));
        listTags.add(new KeyValue("Document",gson.toJson(fpsPaymentRequest.getDocument())));
        //Payment Return Id
//        listTags.add(new KeyValue(AccountingTagsTypes.FPS_PAYMENT_RETURNED_ID.getTag(), null));
        //Payment Return Code
//        listTags.add(new KeyValue(AccountingTagsTypes.FPS_PAYMENT_RETURN_CODE.getTag(), null));
        // Transaction Identification
        //listTags.add(new KeyValue(TransactionLogTagsTypes.TRANSACTION_ID.getTag(), fpsInboundMessage.get));

        Boolean isException = fpsPaymentRequest==null || fpsSanctionAction==null;
        listTags.add(new KeyValue(AccountingTagsTypes.FPS_PAYMENT_RETURNED_EXCEPTION.getTag(), isException.toString()));

        return listTags;
    }

    private Node getProcessorNode(String internalAccountId){
        Node processorNode=null;
        if (!internalAccountId.equals(StringUtils.EMPTY)) {
            processorNode=processorCluster.nodeByAccount(internalAccountId);
        }else {
            Map<Integer, Node> nodes = processorCluster.getNodes();
            List<Node> listNodes = new ArrayList<Node>(nodes.values());
            Integer randomNode = 0 + new Random().nextInt((listNodes.size() - 0) + 1);
            LOG.info("get random proccesor Node for exception processing {}",randomNode);
            processorNode = listNodes.get(randomNode);
        }
        return processorNode;
    }

}
