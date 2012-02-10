/**
 * 
 */
package com.swengle.phoebe.test;

import static org.junit.Assert.*;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMappingException;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.key.InvalidEntityKeyException;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.test.model.EntityWithHashKey;
import com.swengle.phoebe.test.model.EntityWithJustHashKey;
import com.swengle.phoebe.test.model.EntityWithJustHashKeyAndJustRangeKey;


/**
 * @author Administrator
 *
 */
public class TestEntityKey {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link com.swengle.phoebe.key.EntityKey#create(java.lang.Class, java.lang.Object)}.
	 */
	@Test
	public void testObjectKeyCreation() {
		try {
			EntityKey.create(TestEntityKey.class);
			fail("Expected failure when creating Key on non-DynamoDB model class");
		} catch (DynamoDBMappingException e) {
			// do nothing
		}

		try {
			EntityKey.create(new Object());
			fail("Expected failure when creating Key on non-DynamoDB model object");
		} catch (DynamoDBMappingException e) {
			// do nothing
		}
		
		EntityWithJustHashKey entityWithJustHashKey = new EntityWithJustHashKey();
		try {
			EntityKey.create(entityWithJustHashKey);
			fail("Expected failure when creating Key on EntityWithJustHashKey with hashKey not set");
		} catch (InvalidEntityKeyException e) {
			// do nothing
		}
		
		String hashKey = new ObjectId().toString();
		entityWithJustHashKey.setHashKey(hashKey);
		EntityKey<EntityWithJustHashKey> key = EntityKey.create(entityWithJustHashKey);
		
		Assert.assertTrue(key.getKindClass().equals(EntityWithJustHashKey.class));
		Assert.assertTrue(key.getHashKey().equals(entityWithJustHashKey.getHashKey()));
		Assert.assertTrue(key.getRangeKey() == null);
		Assert.assertTrue(key.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(EntityWithJustHashKey.class).tableName()));
		
		key = EntityKey.create(EntityWithJustHashKey.class, hashKey);
		Assert.assertTrue(key.getKindClass().equals(EntityWithJustHashKey.class));
		Assert.assertTrue(key.getHashKey().equals(hashKey));
		Assert.assertTrue(key.getRangeKey() == null);
		Assert.assertTrue(key.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(EntityWithHashKey.class).tableName()));
		
		
		EntityWithJustHashKeyAndJustRangeKey entityWithJustHashKeyAndJustRangeKey = new EntityWithJustHashKeyAndJustRangeKey();
		try {
			EntityKey.create(entityWithJustHashKeyAndJustRangeKey);
			fail("Expected failure when creating Key on EntityWithJustHashKeyAndJustRangeKey with unset hashKey and unset rangeKey");
		} catch (InvalidEntityKeyException e) {
			// do nothing
		}
		
		EntityKey<EntityWithJustHashKeyAndJustRangeKey> key2;
		entityWithJustHashKeyAndJustRangeKey.setHashKey(hashKey);
		
		try {
			key2 = EntityKey.create(entityWithJustHashKeyAndJustRangeKey);
			fail("Expected failure creating Key on EntityWithJustHashKeyAndJustRangeKey with unset rangeKey");
		} catch (InvalidEntityKeyException e) {
			// ignore
		}
			
		
		entityWithJustHashKeyAndJustRangeKey.setRangeKey(hashKey);
		key2 = EntityKey.create(entityWithJustHashKeyAndJustRangeKey);
		Assert.assertTrue(key2.getKindClass().equals(EntityWithJustHashKeyAndJustRangeKey.class));
		Assert.assertTrue(key2.getHashKey().equals(entityWithJustHashKeyAndJustRangeKey.getHashKey()));
		Assert.assertTrue(key2.getRangeKey().equals(hashKey));
		Assert.assertTrue(key2.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(EntityWithJustHashKeyAndJustRangeKey.class).tableName()));
		
		try {
			key2 = EntityKey.create(EntityWithJustHashKeyAndJustRangeKey.class, hashKey, null);
			fail("Expected failure creating Key on EntityWithJustHashKeyAndJustRangeKey class with unset rangeKey");
		} catch (InvalidEntityKeyException e) {
			// ignore
		}
		
		
		key2 = EntityKey.create(EntityWithJustHashKeyAndJustRangeKey.class, hashKey, hashKey);
		Assert.assertTrue(key2.getKindClass().equals(EntityWithJustHashKeyAndJustRangeKey.class));
		Assert.assertTrue(key2.getHashKey().equals(hashKey));
		Assert.assertTrue(key2.getRangeKey().equals(hashKey));
		Assert.assertTrue(key2.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(EntityWithJustHashKeyAndJustRangeKey.class).tableName()));
		
		EntityKey<EntityWithJustHashKeyAndJustRangeKey> keyTop = EntityKey.create(EntityWithJustHashKeyAndJustRangeKey.class, hashKey, hashKey);
		EntityKey<EntityWithJustHashKeyAndJustRangeKey> keyTopDitto = EntityKey.create(EntityWithJustHashKeyAndJustRangeKey.class, hashKey, hashKey);
		EntityKey<EntityWithJustHashKeyAndJustRangeKey> keyBottom = EntityKey.create(EntityWithJustHashKeyAndJustRangeKey.class, hashKey, new ObjectId().toString());

		Assert.assertTrue(keyTop.equals(keyTopDitto));
		Assert.assertTrue(keyTop.compareTo(keyTopDitto) == 0);
		Assert.assertTrue(keyTop.compareTo(keyBottom) == -1);
		Assert.assertTrue(keyBottom.compareTo(keyTop) == 1);
		Assert.assertTrue(keyTop.hashCode() == keyTopDitto.hashCode());

	}
}
