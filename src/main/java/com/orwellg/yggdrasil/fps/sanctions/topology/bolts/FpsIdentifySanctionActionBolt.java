package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import com.orwellg.umbrella.avro.types.payment.fps.FPSSanctionsAction;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.yggdrasil.fps.sanctions.scylla.entities.FpsPaymentRequest;
import com.orwellg.yggdrasil.fps.sanctions.topology.FpsSanctionsTopology;


public class FpsIdentifySanctionActionBolt extends BasicRichBolt {

	private static final long serialVersionUID = 1L;

	public final static Logger LOG = LogManager.getLogger(FpsIdentifySanctionActionBolt.class);


    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }

    @Override
    public void declareFieldsDefinition() {
        	addFielsDefinition(FpsSanctionsTopology.FPS_SANCTION_ACTION_PASS_STREAM, Arrays.asList(new String[] {"key", "processId", "result", "commandName", "eventName", "eventData", "topic", "sanctionAction","fpsPaymentRequest"}));
        	addFielsDefinition(FpsSanctionsTopology.FPS_SANCTION_ACTION_FAIL_STREAM, Arrays.asList(new String[] {"key", "processId", "result", "commandName", "eventName", "eventData", "topic", "sanctionAction","fpsPaymentRequest"}));
            addFielsDefinition(FpsSanctionsTopology.FPS_SANCTION_FAIL_RETURN_STREAM, Arrays.asList(new String[] {"key", "processId", "result", "commandName", "eventName", "eventData", "topic", "sanctionAction","fpsPaymentRequest"}));

    }

    @Override
    public void execute(Tuple tuple) {
        String eventKey = null;
        String pmtId = null;

        try {
            FpsPaymentRequest data = (FpsPaymentRequest) tuple.getValueByField("eventData");
            FPSSanctionsAction sanctionAction = (FPSSanctionsAction) tuple.getValueByField("sanctionAction");
            FpsPaymentRequest fpsPaymentRequest = (FpsPaymentRequest) tuple.getValueByField("fpsPaymentRequest");
            eventKey = (String) tuple.getValueByField("key");
            pmtId = (String) tuple.getValueByField("processId");
            LOG.info("[PmtId: {}] Received fps sanction to identify sanction action with event key {}. Sanction Action: {}", pmtId, eventKey, sanctionAction.getSanctionsAction());
            Map<String, Object> values = new HashMap<>();

            values.put("key", eventKey);
            values.put("processId", pmtId);
            values.put("sanctionAction", sanctionAction);
            values.put("eventData", data);
            values.put("fpsPaymentRequest", fpsPaymentRequest);
            
            if (FpsSanctionsTopology.FPS_SANCTION_ACTION_PASS.equals(sanctionAction.getSanctionsAction())) {
                send(FpsSanctionsTopology.FPS_SANCTION_ACTION_PASS_STREAM, tuple, values);
            } else {
                send(FpsSanctionsTopology.FPS_SANCTION_FAIL_RETURN_STREAM, tuple, values);
                send(FpsSanctionsTopology.FPS_SANCTION_ACTION_FAIL_STREAM, tuple, values);
            }

        } catch (Exception ex) {
	        	LOG.error("[PmtId: {}] Error obtaining payment type for inbound payment. Error message: {}", pmtId, ex.getMessage(), ex);
	        	error(ex, tuple);
        }

    }

}
