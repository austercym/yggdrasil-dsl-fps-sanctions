package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;


import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.payment.fps.FPSSanctionsAction;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.InternalAccountRepository;
import com.orwellg.umbrella.commons.repositories.scylla.ProductRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.InternalAccountRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.ProductRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.beans.AccountSchemeNameTypes;
import com.orwellg.umbrella.commons.types.fps.RejectionCode;
import com.orwellg.umbrella.commons.types.scylla.entities.IBANAccount;
import com.orwellg.umbrella.commons.types.scylla.entities.InternalAccount;
import com.orwellg.umbrella.commons.types.scylla.entities.Product;
import com.orwellg.umbrella.commons.types.scylla.entities.SortCodeAccount;
import com.orwellg.umbrella.commons.utils.enums.ProductStatus;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfigFactory;
import com.orwellg.yggdrasil.fps.sanctions.exception.CustomerAccountDataException;
import com.orwellg.yggdrasil.fps.sanctions.exception.IBANNotFoundException;
import com.orwellg.yggdrasil.fps.sanctions.exception.SortCodeNotFoundException;
import com.orwellg.yggdrasil.fps.sanctions.messages.FpsErrorMessage;
import com.orwellg.yggdrasil.fps.sanctions.messages.FpsInboundMessage;
import com.orwellg.yggdrasil.fps.sanctions.scylla.entities.FpsPaymentRequest;

public class FpsValidateCustomerAccountBolt extends BasicRichBolt {

    public final static Logger LOG = LogManager.getLogger(FpsValidateCustomerAccountBolt.class);

    private static final long serialVersionUID = 1L;

    private final Integer VALID = 0;

    public static final String FPS_VALIDATE_ACCOUNT_ERROR_STREAM = "FPS_VALIDATE_ACCOUNT_ERROR_STREAM";

    private ProductRepository productRepository;
    private InternalAccountRepository internalAccountRepository;
    
    private ScyllaParams topologyScyllaParams;
    private String scyllaKeyspace;

    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        topologyScyllaParams = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig().getScyllaConfig().getScyllaParams();
        scyllaKeyspace = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig().getScyllaConfig().getScyllaParams().getKeyspace();
        		
        internalAccountRepository = new InternalAccountRepositoryImpl(topologyScyllaParams, scyllaKeyspace);
        productRepository = new ProductRepositoryImpl(topologyScyllaParams, scyllaKeyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData", "fpsPaymentRequest", "sanctionAction"}));
        addFielsDefinition(FPS_VALIDATE_ACCOUNT_ERROR_STREAM, Arrays.asList(new String[] {"key", "processId", "eventData", "errors"}));
    }

    @Override
    public void execute(Tuple tuple) {
	 	LOG.info("Getting ready for executing ValidateCustomerAccount Bolt. Tuple values: {}", tuple);
        String internalAccountId = StringUtils.EMPTY;
        Map<String, Object> values = new HashMap<>();

        String eventKey = null;
        String pmtId = null;
        FpsInboundMessage fpsInboundMessage = null;
        FpsPaymentRequest fpsPaymentRequest = null;
        FPSSanctionsAction fpsSanctionsAction = null;
        
        try {
        	eventKey = tuple.getStringByField("key");
            pmtId = tuple.getStringByField("processId");
            fpsPaymentRequest = (FpsPaymentRequest) tuple.getValueByField("fpsPaymentRequest");
            fpsSanctionsAction = (FPSSanctionsAction) tuple.getValueByField("eventData");

            LOG.info("[PmtId: {}] Received FPS inbound payment to validate customer account with event key {}", pmtId, eventKey);
            
            fpsInboundMessage = new FpsInboundMessage();
            fpsInboundMessage.setPaymentDocument(fpsPaymentRequest.getDocument());
            
            try {
                if (isNotBlank(fpsInboundMessage.getCreditorSortAccount())) {
                	// Customer Account is an UK account
                    if (isNotBlank(fpsInboundMessage.getCreditorSortCode())) {
                    	// SortCode Account
                        String internalAccountAppended = new StringBuilder().append(fpsInboundMessage.getCreditorSortCode()).append(fpsInboundMessage.getCreditorSortAccount()).toString();
                        LOG.info("[PmtId: {}] Customer account is an UK Sort Code account {}", pmtId, internalAccountAppended);
                        InternalAccount internalAccount = internalAccountRepository.getInternalAccount(internalAccountAppended, AccountSchemeNameTypes.UK);
                        if (internalAccount==null){
                            throw new SortCodeNotFoundException(String.format("Sort Code %s Not found",fpsInboundMessage.getCreditorSortCode()));
                        }
                        internalAccountId = internalAccount.getInternalAccountId();
                    } else {
                    	// Only Account
                        LOG.info("[PmtId: {}] Customer account is an UK account without sort code {}", pmtId, fpsInboundMessage.getCreditorSortAccount());
                        SortCodeAccount sortCodeAccount = internalAccountRepository.getSortCodeAccount(fpsInboundMessage.getCreditorSortAccount());
                        if (sortCodeAccount==null) throw new SortCodeNotFoundException(String.format("Sort Code Account %s Not found",fpsInboundMessage.getCreditorSortAccount()));
                        internalAccountId = sortCodeAccount.getInternalAccountId();
                    }
                } else if (isNotBlank(fpsInboundMessage.getCreditorIBAN())) {
                	// Customer Account is an IBAN
                    LOG.info("[PmtId: {}] Customer account is an IBAN account {}", pmtId, fpsInboundMessage.getCreditorIBAN());
                    IBANAccount ibanAccount = internalAccountRepository.getIbanAccount(fpsInboundMessage.getCreditorIBAN());
                    if (ibanAccount==null) throw new IBANNotFoundException(String.format("IBAN %s Not found",fpsInboundMessage.getCreditorIBAN()));
                    internalAccountId = ibanAccount.getInternalAccountId();
                } else {
                	throw new CustomerAccountDataException("Customer account data (Sort Code Account or IBAN) not found on inbound payment"); 
                }
                LOG.info("[PmtId: {}] Found customer account internal id: {}", pmtId, internalAccountId);
         
                Integer validAccount = validateAccountStatus(pmtId, internalAccountId);
                values.put("key", pmtId);
                values.put("processId", pmtId);
                values.put("eventData", fpsPaymentRequest);
                values.put("sanctionAction", fpsSanctionsAction);
                values.put("fpsPaymentRequest", fpsPaymentRequest);

                LOG.info("[PmtId: {}] Customer account {} validated", pmtId, internalAccountId);
                LOG.info("Sending tuple values. Tuple: {}", new Gson().toJson(values));

                if (validAccount.equals(VALID)){
                    send(tuple, values);
                } else {
                    values.put("errors", new FpsErrorMessage(validAccount, FpsErrorMessage.FPS_NOT_VALID_ACCOUNT));
                    send(FPS_VALIDATE_ACCOUNT_ERROR_STREAM,tuple, values);
                }
                
            } catch (SortCodeNotFoundException | IBANNotFoundException | CustomerAccountDataException accex ) {
                LOG.error("[PmtId: {}]-- {}", pmtId, accex.getMessage());
                values.put("key", pmtId);
                values.put("errors", new FpsErrorMessage(RejectionCode.REJECT_1114.getCode(), FpsErrorMessage.FPS_NOT_EXISTING_ACCOUNT));
                send(FPS_VALIDATE_ACCOUNT_ERROR_STREAM, tuple, values);
            }
            
        } catch(Exception ex) {
	        	LOG.error("[PmtId: {}] Error validating customer account for inbound payment. Error message: {}", pmtId, ex.getMessage(), ex);
	        	error(ex, tuple);
        }
    }


    private Integer validateAccountStatus(String paymentId, String internalAccountId){
        if (internalAccountId != null) {
            Product account = productRepository.getProduct(internalAccountId);
            if (ProductStatus.PRODUCT_STATUS_CLOSED.getStatus().equals(account.getStatus())) {
            	LOG.error("[PmtId: {}] AccountId {} not valid. Account Closed", paymentId, internalAccountId);
                return RejectionCode.REJECT_1160.getCode();
            } else if (ProductStatus.PRODUCT_STATUS_BLOCKED.getStatus().equals(account.getStatus())) {
                LOG.error("[PmtId: {}] AccountId {} not valid. Account Blocked", paymentId, internalAccountId);
                return RejectionCode.REJECT_1161.getCode();
            } 
        }
        return VALID;
    }
}
