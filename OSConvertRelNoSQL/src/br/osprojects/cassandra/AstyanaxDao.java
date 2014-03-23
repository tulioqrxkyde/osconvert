package br.osprojects.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxDao {

	private Keyspace keyspace;
	private AstyanaxContext<Keyspace> astyanaxContext;

	public AstyanaxDao(String host, String keyspace) {
		try {
			this.astyanaxContext = new AstyanaxContext.Builder()
					.forCluster("ClusterName")
					.forKeyspace(keyspace)
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl()
									.setDiscoveryType(NodeDiscoveryType.NONE))
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(
									"MyConnectionPool").setMaxConnsPerHost(1)
									.setSeeds(host))
					.withConnectionPoolMonitor(
							new CountingConnectionPoolMonitor())
					.buildKeyspace(ThriftFamilyFactory.getInstance());
			this.astyanaxContext.start();
			this.keyspace = this.astyanaxContext.getEntity();

			// test the connection
			this.keyspace.describeKeyspace();
		} catch (Throwable e) {
			System.out.println("Could not connect to cassandra. Erro: " + e);
			System.exit(-1);
		}
	}

	public void cleanup() {
		this.astyanaxContext.shutdown();
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}
}
