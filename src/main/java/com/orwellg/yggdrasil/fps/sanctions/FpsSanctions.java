package com.orwellg.yggdrasil.fps.sanctions;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;

import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfig;
import com.orwellg.yggdrasil.fps.sanctions.config.FpsSanctionsTopologyConfigFactory;
import com.orwellg.yggdrasil.fps.sanctions.topology.FpsSanctionsTopology;

public class FpsSanctions {

	private final static Logger LOG = LogManager.getLogger();


	public static void main(String[] args) throws Exception {
		boolean local = false;

		if (args.length >= 1 && args[0].equals("local")) {
			LOG.info("*********** Local parameter received, will work with LocalCluster ************");
			local = true;
		}

		FpsSanctionsTopology topology = new FpsSanctionsTopology();

		if (local) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topology.name(), config(), topology.load());
			Thread.sleep(6000000L);
			localCluster.shutdown();
		} else {
			StormSubmitter.submitTopology(topology.name(), config(), topology.load());
		}
	}
	

    private static Config config() {
        FpsSanctionsTopologyConfig config = FpsSanctionsTopologyConfigFactory.getDSLTopologyConfig();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(config.getTopologyMaxTaskParallelism());
        conf.setNumWorkers(config.getTopologyNumWorkers());
        return conf;
    }
}
