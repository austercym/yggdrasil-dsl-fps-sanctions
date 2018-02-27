package com.orwellg.yggdrasil.fps.sanctions.scylla;

import com.datastax.driver.core.*;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.fps.PaymentStatus;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


public class EiscdFasterPaymentsNoSqlDao {

	private final static Logger LOG = LogManager.getLogger(EiscdFasterPaymentsNoSqlDao.class);

	protected ScyllaManager man;

	protected Session ses;


	public EiscdFasterPaymentsNoSqlDao() {
	}

	public EiscdFasterPaymentsNoSqlDao(String scyllaNodes,String keySpace) {
		ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams();
		man = ScyllaManager.getInstance(scyllaParams);
		this.ses = man.getSession(scyllaParams.getKeyspace());
	}

	public PaymentStatus paymentStatus(String sortCode) {

		String selectQuery = "select * from ipagoo.eiscd_fasterpayment where sortcode = ? LIMIT 1";
		PreparedStatement st = ses.prepare(selectQuery);
		BoundStatement select = st.bind(sortCode);
		ResultSet rs = ses.execute(select);
		Row result = rs.one();
        PaymentStatus resultType = null;
        if (result!=null){
            resultType = PaymentStatus.valueCaseSafe(result.getString("status"));
        }
		return resultType;
	}

}
