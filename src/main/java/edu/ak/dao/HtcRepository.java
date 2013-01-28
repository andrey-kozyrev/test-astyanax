package edu.ak.dao;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * User: AndreyK
 * Date: 1/27/13
 * Time: 3:39 PM
 */
public class HtcRepository {

	private final AstyanaxContext<Keyspace> _context;
	private final Keyspace _keyspace;

	public Keyspace getKeyspace() {
		return _keyspace;
	}

	public HtcRepository() {
		_context = new AstyanaxContext.Builder()
			.forCluster("htc")
			.forKeyspace("test")
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
				.setDiscoveryType(NodeDiscoveryType.NONE)
			)
			.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("htc")
				.setPort(9160)
				.setMaxConnsPerHost(3)
				.setSeeds("ENCTADB")
			)
			.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
			.buildKeyspace(ThriftFamilyFactory.getInstance());
		_context.start();

		_keyspace = _context.getEntity();
	}


}
