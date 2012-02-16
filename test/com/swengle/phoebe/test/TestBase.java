/**
 * 
 */
package com.swengle.phoebe.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.BeforeClass;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.concurrent.FutureResult;
import com.swengle.phoebe.datastore.DuplicateTableException;
import com.swengle.phoebe.query.Query;
import com.swengle.phoebe.test.model.EntityWithJustHashKey;
import com.swengle.phoebe.test.model.EntityWithJustHashKeyAndJustRangeKey;


/**
 * @author Administrator
 * 
 */
public class TestBase {
	public static Phoebe PHOEBE;
	private static final Log LOG = LogFactory.getLog(TestBase.class);

	
	@BeforeClass
	public static void globalSetup() throws IOException {
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		
		AWSCredentials credentials = new PropertiesCredentials(
				TestBase.class.getClassLoader().getResourceAsStream("AwsCredentials.properties"));
		PHOEBE = new Phoebe(credentials, clientConfiguration);
		PHOEBE.getClient().setEndpoint(
				"http://dynamodb.us-east-1.amazonaws.com");
		createTestTables();
	}
	
	@Before
	public void setUp() {

	}
	
	protected void emptyAll() {
		empty(EntityWithJustHashKey.class);
		empty(EntityWithJustHashKeyAndJustRangeKey.class);
	}

	@SuppressWarnings("unused")
	private void deleteTestTables() {
		PHOEBE.getDatastore().dropTable(EntityWithJustHashKey.class);
		PHOEBE.getDatastore().dropTable(EntityWithJustHashKeyAndJustRangeKey.class);
	}


	private static void createTestTables() {
		FutureResult<Void> create1FutureResult = PHOEBE.getAsyncDatastore().createTable(EntityWithJustHashKey.class, 128L, 128L);
		FutureResult<Void> create2FutureResult = PHOEBE.getAsyncDatastore().createTable(EntityWithJustHashKeyAndJustRangeKey.class, 128L, 128L);
		try {
			create1FutureResult.now();
		} catch (DuplicateTableException e) {
			LOG.warn(e);
		}
		try {
			create2FutureResult.now();
		} catch (DuplicateTableException e) {
			LOG.warn(e);
		}
	}
	
	private <T> void empty(Class<T> kindClass) {
		LOG.info("Emptying the table for: " + kindClass);
		Query<T>  qry = PHOEBE.createScanQuery(kindClass);
		LOG.info("Found " + qry.count() + " entities to remove.");
		PHOEBE.getDatastore().delete(qry);
		LOG.info("Done emptying the table for: " + kindClass);
	}
	
}
