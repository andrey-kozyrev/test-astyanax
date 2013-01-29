package edu.ak;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import edu.ak.dao.HtcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * User: AndreyK
 * Date: 1/27/13
 * Time: 3:33 PM
 */
public class Application {

	public static class Runner implements Runnable {

		private final static String TEXT;

		private final HtcRepository _repository;

		static {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < 1; ++i) {
				sb.append("The Rows object returned by the query transparently paginates through all rows in the column family. Since queries to the keyspace are actually done through the iteration it is necessary to set an ExceptionCallback for your application to handle the exceptions. Return true from the callback to retry or false to exit the iteration loop.");
			}
			TEXT = sb.toString();
		}

		public Runner(HtcRepository repository) {
			assert repository != null;
			_repository = repository;
		}

		@Override
		public void run() {
			Logger logger = LoggerFactory.getLogger(Application.class);
			try {
				final int count = 10;
				final int user_count = 50;
				long latency = 0;
				Keyspace keyspace = _repository.getKeyspace();
				for (int n = 0; n < user_count; ++n) {
					for (int m = 0; m < user_count; ++m) {
						String key = String.format("USER%d:USER%d", n, m);
						for (int i = 0; i < count; ++i) {
							Date column = new Date();
							latency += keyspace.prepareColumnMutation(HtcRepository.CF_TRACKS, key, column).putValue(TEXT, null).execute().getLatency(TimeUnit.MICROSECONDS);
						}
					}
				}
				logger.debug(String.format("Per-thread insert average: %d milliseconds", latency / user_count / user_count / count / 1000));
			} catch (ConnectionException e) {
				logger.error("Oops...", e);
			}
		}
	}

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(Application.class);
		logger.info("Started");
		HtcRepository repository = new HtcRepository();
		try {
			repository.start();

			final int count = 100;
			List<Thread> runners = new ArrayList<>(count);

			long millis = System.currentTimeMillis();

			for (int i = 0; i < count; ++i) {
				Thread t = new Thread(new Runner(repository));
				runners.add(t);
				t.start();
			}

			for (Thread t: runners) {
				try {
					t.join();
				} catch (InterruptedException e) {
					logger.error("Oops...", e);
				}
			}

			double delta = System.currentTimeMillis() - millis;

			logger.info(String.format("Overall insert average: %f milliseconds", delta / 1000 / count));

		} catch (ConnectionException e) {
			logger.error("Oops...", e);
		}

		repository.stop();
	}
}
