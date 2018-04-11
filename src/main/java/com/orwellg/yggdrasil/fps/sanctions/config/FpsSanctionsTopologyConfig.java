package com.orwellg.yggdrasil.fps.sanctions.config;

import com.orwellg.umbrella.commons.beans.config.kafka.PublisherKafkaConfiguration;
import com.orwellg.yggdrasil.commons.storm.topology.config.DSLTopologyConfig;

public class FpsSanctionsTopologyConfig extends DSLTopologyConfig {
	
	
	public static final String PROPERTY_TOPIC_ACCTNG_RESPONSE = "accounting.response.topic";

	public FpsSanctionsTopologyConfig() {
		this(DEFAULT_PROPERTIES_FILE);
	}

	public FpsSanctionsTopologyConfig(String propertiesFile) {
		super(propertiesFile);
	}
	
	/**
	 * @return publisher config response topic for the topology 
	 * 			(read from property file and default values).
	 */
	public PublisherKafkaConfiguration getKafkaPublisherResponseBoltConfig() {
		PublisherKafkaConfiguration pubConf = new PublisherKafkaConfiguration();

		boolean forceSet = true;
		setPubConfData(pubConf, forceSet, "response.topic");

		return pubConf;
	}
	
	

}
