package com.orwellg.yggdrasil.fps.sanctions.topology;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopology;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaCommandGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltFieldNameWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfig;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfigFactory;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsAccountingCommandBoltFail;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsAccountingCommandBoltPass;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsCreateReturnResponseBolt;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsIdentifySanctionActionBolt;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsOriginalPaymentBolt;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsProcessBolt;
import com.orwellg.yggdrasil.fps.sanctions.topology.bolts.FpsValidateCustomerAccountBolt;


public class FpsSanctionsTopology extends AbstractTopology {

	private final static Logger LOG = LogManager.getLogger(FpsSanctionsTopology.class);

    private static final String TOPOLOGY_NAME = "dsl-fps-sanctions";
    private static final String KAFKA_EVENT_READER_COMPONENT = "com.orwellg.yggdrasil.dsl.fps.sanctions.response.1";
    
    private static final String FPS_PROCESS_COMPONENT = "fpsProcessEventPayment";

    private static final String FPS_IDENTIFY_SANCTIONACTION_COMPONENT = "fpsIdentifySanctionAction";
    
    private static final String FPS_VALIDATE_SCHEME_COMPONENT = "fpsValidateScheme";
    private static final String FPS_VALIDATE_CUSTOMER_ACCOUNT_COMPONENT = "fpsValidateCustomerAccount";
    private static final String FPS_ACCOUNTING_COMMAND_COMPONENT_PASS = "fpsAccountingCommandPass";
    private static final String FPS_ACCOUNTING_COMMAND_COMPONENT_FAIL = "fpsAccountingCommandFail";
    private static final String FPS_ACCOUNTING_KAFKA_COMMAND_COMPONENT = "fpsAccountingKafkaCommand";
    private static final String FPS_ACCOUNTING_PUBLISH_COMMAND_COMPONENT = "fpsAccountingPublishCommand";
    private static final String FPS_VALIDATION_ERROR_HANDLING = "fpsValidationErrorHandling";
    private static final String FPS_VALIDATION_ERROR_PRODUCER_COMPONENT = "fpsValidationErrorProducer";
    private static final String FPS_RETURN_ORIGINAL_BOLT = "fpsReturnOriginalBolt";
    
    private static final String FPS_CREATE_RETURN_RESPONSE_COMPONENT = "fpsCreateReturnResponse";
    private static final String FPS_PRODUCE_RETURN_RESPONSE_COMPONENT = "fpsProduceCreateReturnResponse";

    private static final String FPS_ERROR_HANDLING = "fpsErrorHandling";
    private static final String FPS_ERROR_PRODUCER_COMPONENT = "fpsErrorProducer";
    
    public static final String FPS_SANCTION_ACTION_PASS = "PASS";
    public static final String FPS_SANCTION_ACTION_FAIL = "FAIL";

    
    public static final String FPS_SANCTION_ACTION_PASS_STREAM              = "SANCTIONACTION_PASS_STREAM";
    public static final String FPS_SANCTION_ACTION_FAIL_STREAM              = "SANCTIONACTION_FAIL_STREAM";

	@Override
	public StormTopology load() {
        // Read configuration params from topology.properties and zookeeper
        FpsSanctionsTopologyConfig config = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig();

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER_COMPONENT, new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

        // Identify Payment Type 
        GBolt<?> fpsProcessBolt = new GRichBolt(FPS_PROCESS_COMPONENT, new FpsProcessBolt(), config.getActionBoltHints());
        fpsProcessBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT, KafkaSpout.EVENT_SUCCESS_STREAM));
        
        //Get Original Payment
        GBolt<?> fpsReturnOriginalPaymentBolt = new GRichBolt(FPS_RETURN_ORIGINAL_BOLT, new FpsOriginalPaymentBolt(), config.getActionBoltHints());
        fpsReturnOriginalPaymentBolt.addGrouping(new ShuffleGrouping(FPS_PROCESS_COMPONENT));
        
        //Validate Customer Account
        GBolt<?> fpsValidateCustomerAccountBolt = new GRichBolt(FPS_VALIDATE_CUSTOMER_ACCOUNT_COMPONENT, new FpsValidateCustomerAccountBolt(), config.getActionBoltHints());
        fpsValidateCustomerAccountBolt.addGrouping(new ShuffleGrouping(FPS_RETURN_ORIGINAL_BOLT));
        
        
        // -------------------------------------------------------
        // Identify Pass/Fail 
        // -------------------------------------------------------
        GBolt<?> fpsIdentifySanctionActionTypeBolt = new GRichBolt(FPS_IDENTIFY_SANCTIONACTION_COMPONENT, new FpsIdentifySanctionActionBolt(), config.getActionBoltHints());
        fpsIdentifySanctionActionTypeBolt.addGrouping(new ShuffleGrouping(FPS_VALIDATE_CUSTOMER_ACCOUNT_COMPONENT));
        
        //If pass
        GBolt<?> fpsAccountingCommandBoltPass = new GRichBolt(FPS_ACCOUNTING_COMMAND_COMPONENT_PASS, new FpsAccountingCommandBoltPass(), config.getActionBoltHints());
        fpsAccountingCommandBoltPass.addGrouping(new ShuffleGrouping(FPS_IDENTIFY_SANCTIONACTION_COMPONENT, FPS_SANCTION_ACTION_PASS_STREAM));
        //If fail
        GBolt<?> fpsAccountingCommandBoltFail = new GRichBolt(FPS_ACCOUNTING_COMMAND_COMPONENT_FAIL, new FpsAccountingCommandBoltFail(), config.getActionBoltHints());
        fpsAccountingCommandBoltFail.addGrouping(new ShuffleGrouping(FPS_IDENTIFY_SANCTIONACTION_COMPONENT, FPS_SANCTION_ACTION_FAIL_STREAM));
        
        //Generate Command (Debit Sanctions, Credit Customer Account)
//        GBolt<?> fpsAccountingCommandBolt = new GRichBolt(FPS_ACCOUNTING_COMMAND_COMPONENT, new FpsAccountingCommandBolt(), config.getActionBoltHints());
//        fpsAccountingCommandBolt.addGrouping(new ShuffleGrouping(FPS_VALIDATE_CUSTOMER_ACCOUNT_COMPONENT));
        GBolt<?> eventAccountingCommandGeneratorBolt = new GRichBolt(FPS_ACCOUNTING_KAFKA_COMMAND_COMPONENT, new KafkaCommandGeneratorBolt(), config.getKafkaSpoutHints());
        eventAccountingCommandGeneratorBolt.addGrouping(new ShuffleGrouping(FPS_ACCOUNTING_COMMAND_COMPONENT_PASS));
        eventAccountingCommandGeneratorBolt.addGrouping(new ShuffleGrouping(FPS_ACCOUNTING_COMMAND_COMPONENT_FAIL));
        GBolt<?> kafkaAccountingCommandProducerBolt = new GRichBolt(FPS_ACCOUNTING_PUBLISH_COMMAND_COMPONENT, new KafkaBoltFieldNameWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getActionBoltHints());
        kafkaAccountingCommandProducerBolt.addGrouping(new ShuffleGrouping(FPS_ACCOUNTING_KAFKA_COMMAND_COMPONENT));
        
        
        // Validation Errors Handling
//      GBolt<?> fpsValidationErrorHandlingBolt = new GRichBolt(FPS_VALIDATION_ERROR_HANDLING, new FpsErrorHandlingBolt(), config.getActionBoltHints());
//      fpsValidationErrorHandlingBolt.addGrouping(new ShuffleGrouping(FPS_IDENTIFY_PAYMENTTYPE_COMPONENT, FpsIdentifyPaymentTypeBolt.PAYMENTTYPE_ERROR_STREAM));
//      fpsValidationErrorHandlingBolt.addGrouping(new ShuffleGrouping(FPS_VALIDATE_SCHEME_COMPONENT, FpsValidateSchemeBolt.VALIDATE_SCHEME_ERROR_STREAM));
//      fpsValidationErrorHandlingBolt.addGrouping(new ShuffleGrouping(FPS_VALIDATE_CUSTOMER_ACCOUNT_COMPONENT, FpsValidateCustomerAccount.FPS_VALIDATE_ACCOUNT_ERROR_STREAM));
//
//      GBolt<?> kafkaValidationErrorEventProducerBolt = new GRichBolt(FPS_VALIDATION_ERROR_PRODUCER_COMPONENT, new KafkaBoltFieldNameWrapper(config.getKafkaPublisherRejectBoltConfig(), String.class, String.class).getKafkaBolt(), config.getActionBoltHints());
//      kafkaValidationErrorEventProducerBolt.addGrouping(new ShuffleGrouping(FPS_VALIDATION_ERROR_HANDLING));
        
        //Generate return
//        GBolt<?> fpsCreateReturnResponseBolt = new GRichBolt(FPS_CREATE_RETURN_RESPONSE_COMPONENT, new FpsCreateReturnResponseBolt(), config.getActionBoltHints());
//        fpsCreateReturnResponseBolt.addGrouping(new ShuffleGrouping(FPS_ACCOUNTING_COMMAND_COMPONENT));
        
        // Topology Error Handling
        GBolt<?> fpsErrorHandlingBolt = new GRichBolt(FPS_ERROR_HANDLING, new EventErrorBolt(), config.getActionBoltHints());
        fpsErrorHandlingBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT, KafkaSpout.EVENT_ERROR_STREAM));
        
        GBolt<?> kafkaEventErrorProducer = new GRichBolt(FPS_ERROR_PRODUCER_COMPONENT, new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getActionBoltHints());
        kafkaEventErrorProducer.addGrouping(new ShuffleGrouping(FPS_ERROR_HANDLING));
        LOG.info("Config ok");
        

        // Topology
        StormTopology topology = TopologyFactory.generateTopology(kafkaEventReader,
                Arrays.asList(new GBolt[] {fpsProcessBolt,
                                         fpsReturnOriginalPaymentBolt, fpsValidateCustomerAccountBolt, fpsIdentifySanctionActionTypeBolt,
                                         fpsAccountingCommandBoltPass, fpsAccountingCommandBoltFail, eventAccountingCommandGeneratorBolt, kafkaAccountingCommandProducerBolt,
//                							fpsValidationErrorHandlingBolt, kafkaValidationErrorEventProducerBolt,
//                							fpsCreateReturnResponseBolt
                							fpsErrorHandlingBolt, kafkaEventErrorProducer }));

        LOG.info("{} Topology created, submitting it to storm...", TOPOLOGY_NAME);

        return topology;

    }

	@Override
	public String name() {
		return TOPOLOGY_NAME;
	}
}
