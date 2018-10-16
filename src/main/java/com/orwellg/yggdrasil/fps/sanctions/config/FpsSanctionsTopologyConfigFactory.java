package com.orwellg.yggdrasil.fps.sanctions.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.orwellg.yggdrasil.commons.storm.topology.config.DSLTopologyConfigFactory;


public class FpsSanctionsTopologyConfigFactory {
	private final static Logger LOG = LogManager.getLogger(DSLTopologyConfigFactory.class);

	protected static FpsSanctionsTopologyConfig dslTopologyConfig;
	
	protected static synchronized void initDSLTopologyConfig(String propertiesFile, String zookeeperHost) {
		
		if (dslTopologyConfig == null) {
			if (propertiesFile != null) {
				LOG.info("Initializing topology with propertiesFile {}", propertiesFile);
				dslTopologyConfig = new FpsSanctionsTopologyConfig(propertiesFile);
			} else {
				LOG.info("Initializing topology with propertiesFile DEFAULT_PROPERTIES_FILE");
				dslTopologyConfig = new FpsSanctionsTopologyConfig();
			}
			try {
				dslTopologyConfig.start(zookeeperHost);
			} catch (Exception e) {
				LOG.error("Topology configuration params cannot be started. The system will work with default parameters. Message: {}",  e.getMessage(),  e);
			}
		}		
	}
	
	/**
	 * Loads config from propertiesFile and zookeeper.<br/>
	 * Use it for testing or with a topology-specific properties file name.
	 * @return TopologyConfig initialized with propertiesFile (a properties file with at least "zookeeper.host" property).
	 * @see DSLTopologyConfig
	 */
	public static synchronized FpsSanctionsTopologyConfig getDSLTopologyConfig(String propertiesFile, String zookeeperHost) {
		initDSLTopologyConfig(propertiesFile, zookeeperHost);
		return dslTopologyConfig;
	}

	/**
	 * Loads config from TopologyConfig.DEFAULT_PROPERTIES_FILE and zookeeper.<br/>
	 * This is the usual way to instantiate TopologyConfig.
	 * @return TopologyConfig initialized with TopologyConfig.DEFAULT_PROPERTIES_FILE (a properties file with at least "zookeeper.host" property).
	 * @see DSLTopologyConfig
	 */
	public static synchronized FpsSanctionsTopologyConfig getDSLTopologyConfig(String zookeeperHost) {
		initDSLTopologyConfig(null, zookeeperHost);
		return dslTopologyConfig;
	}
	
	/**
	 * Set topologyConfig ready for a new initialization in getTopologyConfig().<br/>
	 * Useful for testing.
	 */
	public static void resetDSLTopologyConfig() {
		dslTopologyConfig = null;
	}

}
