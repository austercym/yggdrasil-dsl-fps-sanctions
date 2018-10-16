package com.orwellg.yggdrasil.fps.sanctions.topology;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;

import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopologyMain;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfig;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfigFactory;

public class FpsSanctions extends AbstractTopologyMain {

	private final static Logger LOG = LogManager.getLogger();


	public static void main(String[] args) throws Exception {
		boolean local = false;
		String zookeeperhost = "";
		
		if (args.length >= 1 && args[0].equals("local")) {
			LOG.info("*********** Local parameter received, will work with LocalCluster ************");
			local = true;
		}

		FpsSanctionsTopology topology = new FpsSanctionsTopology();

		if (local) {
			zookeeperhost = args[1];
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topology.name(), config(zookeeperhost), topology.load(zookeeperhost));
			Thread.sleep(6000000L);
			localCluster.shutdown();
		} else {
			if(args.length >= 1) {
				zookeeperhost = args[0];
				LOG.info("*********** Set Zookeeper host by parameter {} ************", zookeeperhost);
				StormSubmitter.submitTopology(topology.name(), config(zookeeperhost), topology.load(zookeeperhost));
			}
			else {
				LOG.error("Not received parameter with zookeeper connection string");
				System.exit(-1);
			}
		}
	}
	

    private static Config config(String zookeeper) {
        FpsSanctionsTopologyConfig config = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig(zookeeper);
        return config(config);
    }
}
