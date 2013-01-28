package edu.ak.dao;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: AndreyK
 * Date: 1/27/13
 * Time: 3:39 PM
 */
public class HtcRepository {

	private static final Logger logger = LoggerFactory.getLogger(HtcRepository.class);
	private final AstyanaxContext<Cluster> _clusterCtx;

	private final static String KEYSPACE_NAME_TEST = "test";
	private Keyspace _keyspace;

	public Keyspace getKeyspace() throws ConnectionException {
		if (_keyspace == null) {
			ensureKeyspace();
		}
		return _keyspace;
	}

	public HtcRepository() {
		_clusterCtx = new AstyanaxContext.Builder()
			.forCluster("htc")
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
					.setDiscoveryType(NodeDiscoveryType.NONE)
			)
			.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("htc")
					.setPort(9160)
					.setMaxConnsPerHost(3)
					.setSeeds("ENCTADB")
			)
			.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
			.buildCluster(ThriftFamilyFactory.getInstance());
		_clusterCtx.start();
	}

	public void init() throws ConnectionException {
		ensureKeyspace();
	}

	private KeyspaceDefinition findKeyspaceDefinition(String name) throws ConnectionException {
		KeyspaceDefinition keyspaceDefinition = null;
		List<KeyspaceDefinition> definitions = _clusterCtx.getEntity().describeKeyspaces();
		for (KeyspaceDefinition definition: definitions) {
			if (definition.getName().equals(name)) {
				keyspaceDefinition = definition;
				break;
			}
		}
		return keyspaceDefinition;
	}

	private void ensureKeyspace() throws ConnectionException {
		_keyspace = _clusterCtx.getEntity().getKeyspace(KEYSPACE_NAME_TEST);
		KeyspaceDefinition keyspaceDefinition = findKeyspaceDefinition(KEYSPACE_NAME_TEST);
		if (keyspaceDefinition == null) {
			logger.info(String.format("Keyspace '%s' does not exist.", KEYSPACE_NAME_TEST));
			Map<String, Object> settings = new ImmutableMap.Builder<String, Object>()
				.put("strategy_options", new ImmutableMap.Builder<String, Object>()
					.put("replication_factor", "1").build()
				)
				.put("strategy_class", "SimpleStrategy")
			.build();
			OperationResult<SchemaChangeResult> opr = _keyspace.createKeyspace(settings);
			logger.info(String.format("Keyspace '%s' created in %d milliseconds.", KEYSPACE_NAME_TEST, opr.getLatency(TimeUnit.MILLISECONDS)));
		}
	}
}
