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
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

	private Keyspace ks_test;
	private final static String KS_NAME_TEST = "test";
	private final Map<String, Object> KS_SETTINGS_TEST = new ImmutableMap.Builder<String, Object>()
		.put("strategy_options", new ImmutableMap.Builder<String, Object>()
				.put("replication_factor", "1").build()
		)
		.put("strategy_class", "SimpleStrategy")
		.build();

	private final static String CF_NAME_TRACKS = "tracks";
	public final static ColumnFamily<String, Date> CF_TRACKS = ColumnFamily.newColumnFamily(
		CF_NAME_TRACKS,
		StringSerializer.get(),
		DateSerializer.get()
	);
	private final static Map<String, Object> CF_SETTINGS_TRACKS = new ImmutableMap.Builder<String, Object>()
		.put("comparator_type", "DateType")
		.put("key_validation_type", "DateType")
		.put("default_validation_class", "UTF8Type")
		.build();


	public Keyspace getKeyspace() throws ConnectionException {
		if (ks_test == null) {
			ensureKeyspace();
		}
		return ks_test;
	}

	public HtcRepository() {
		_clusterCtx = new AstyanaxContext.Builder()
			.forCluster("htc")
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
				.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
			)
			.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("htc")
				.setPort(9160)
				.setMaxConnsPerHost(10)
				.setSeeds("ENCTADB, ENCTSDB, ENCTAAPP, ENCTSAPP1, ENCTSAPP2")
			)
			.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
			.buildCluster(ThriftFamilyFactory.getInstance());
	}

	public void start() throws ConnectionException {
		_clusterCtx.start();
		ensureKeyspace();
	}

	public void stop() {
		_clusterCtx.shutdown();
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
		ks_test = _clusterCtx.getEntity().getKeyspace(KS_NAME_TEST);
		KeyspaceDefinition keyspaceDefinition = findKeyspaceDefinition(KS_NAME_TEST);
		if (keyspaceDefinition == null) {
			OperationResult<SchemaChangeResult> opr = ks_test.createKeyspace(KS_SETTINGS_TEST);
			logger.info(String.format("Keyspace '%s' created in %d milliseconds.", KS_NAME_TEST, opr.getLatency(TimeUnit.MILLISECONDS)));
			keyspaceDefinition = ks_test.describeKeyspace();
		}

		ColumnFamilyDefinition cfd = keyspaceDefinition.getColumnFamily("tracks");
		if (cfd == null) {
			OperationResult<SchemaChangeResult> opr = ks_test.createColumnFamily(CF_TRACKS, CF_SETTINGS_TRACKS);
			logger.info(String.format("Column family '%s' created in %d milliseconds.", CF_NAME_TRACKS, opr.getLatency(TimeUnit.MILLISECONDS)));
		}
	}
}
