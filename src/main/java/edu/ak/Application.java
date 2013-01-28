package edu.ak;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import edu.ak.dao.HtcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: AndreyK
 * Date: 1/27/13
 * Time: 3:33 PM
 */
public class Application {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(Application.class);
		logger.info("Started");
		HtcRepository repository = new HtcRepository();
		Keyspace keyspace = repository.getKeyspace();
		try {
			KeyspaceDefinition kd = keyspace.describeKeyspace();
			String skd = kd.toString();
			System.out.println(skd);
		} catch (ConnectionException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}
}
