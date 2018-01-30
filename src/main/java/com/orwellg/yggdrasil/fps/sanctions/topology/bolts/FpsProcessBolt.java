package com.orwellg.yggdrasil.fps.sanctions.topology.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import com.orwellg.umbrella.avro.types.aml.AMLSanctionsRequest;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.payment.fps.FPSSanctionsAction;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt;


public class FpsProcessBolt extends KafkaEventProcessBolt {

	private static final long serialVersionUID = 1L;
	public final static Logger LOG = LogManager.getLogger(FpsProcessBolt.class);


    @Override
    public void sendNextStep(Tuple tuple, Event event) {

    	String eventKey = null;
    	String pmtId = null;
        try {
            eventKey = event.getEvent().getKey().toString();
            pmtId = event.getProcessIdentifier().getUuid().toString();

            LOG.info("[PmtId: {}] Received sanction message with event key {}", pmtId, eventKey);

            // Get the JSON message with the datas
            AMLSanctionsRequest eventData = gson.fromJson(event.getEvent().getData().toString(), AMLSanctionsRequest.class);
            
            FPSSanctionsAction sanctionAction = new FPSSanctionsAction();
            sanctionAction.setPaymentSystem(eventData.getPaymentSystem());
            sanctionAction.setSanctionsAction(eventData.getSanctionSts());
            sanctionAction.setPaymentId(eventData.getPaymentId());

            LOG.info("[PmtId: {}] Sanction Action Data: {} ", pmtId, sanctionAction.toString());
            LOG.info("[PmtId: {}] Payment System: {} - Payment Id: {} - Sanction Action", pmtId, sanctionAction.getPaymentSystem(), sanctionAction.getPaymentId(), sanctionAction.getSanctionsAction());

            Map<String, Object> values = new HashMap<>();

            values.put("key", pmtId);
            values.put("processId", pmtId);
            values.put("eventData", sanctionAction);

            LOG.info("Sending tuple values. Tuple: {}", values);
            send(tuple, values);

            LOG.info("[PmtId: {}] fps message sent to identify type", pmtId);
        } catch (Exception e){
            LOG.error("[PmtId: {}] Error processing inbound Faster Payment. Message: {}", pmtId, e.getMessage(), e);
        }

    }


}
