/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.query.RangeQuery;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.result.UpdateResult;
import com.swengle.phoebe.test.model.EntityWithHashKeyAndRangeKey;


/**
 * @author Administrator
 *
 */
public class TestRangeQuery extends TestBase {
	private static final String TOP_RANGE_ID = new ObjectId().toString();
	private static final String MIDDLE_RANGE_ID = new ObjectId().toString();
	private static final String BOTTOM_RANGE_ID = new ObjectId().toString();
	private Phoebe phoebe = TestBase.PHOEBE;
	
	@Test
	public void testQuerying() {
		emptyAll();
		Set<String> set = new HashSet<String>();
		set.add("A");
		set.add("B");
		set.add("C");
		
		// object 1
		EntityWithHashKeyAndRangeKey obj = new EntityWithHashKeyAndRangeKey();
		obj.setHashKey("RangeTest");
		obj.setRangeKey(TOP_RANGE_ID);
		obj.setString(TOP_RANGE_ID);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeKey(MIDDLE_RANGE_ID);
		obj.setString(MIDDLE_RANGE_ID);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeKey(BOTTOM_RANGE_ID);
		obj.setString(BOTTOM_RANGE_ID);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 4
		obj.setRangeKey(new ObjectId().toString());
		obj.setString(null);
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 5
		obj.setRangeKey(new ObjectId().toString());
		obj.setString(null);
		obj.getSet().remove("B");
		phoebe.getDatastore().put(obj);
		
		RangeQuery<EntityWithHashKeyAndRangeKey> query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		Assert.assertTrue(query.count() == 5);
		
		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().greaterThan(TOP_RANGE_ID);
		Assert.assertTrue(query.count() == 4);

		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().greaterThanOrEq(TOP_RANGE_ID);
		Assert.assertTrue(query.count() == 5);
		
		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().lessThan(BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().lessThanOrEq(BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().equal(BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().between(TOP_RANGE_ID, BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 3);
		
		
		query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");;
		query.withExclusiveStartKey().equal("RangeTest", MIDDLE_RANGE_ID);
		Assert.assertTrue(query.count() == 3);

	}
	

	@Test
	public void testUpdating() {
		RangeQuery<EntityWithHashKeyAndRangeKey> queryAll = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		Assert.assertTrue(queryAll.count() == 5);
		
		RangeQuery<EntityWithHashKeyAndRangeKey> query = phoebe.createRangeQuery(EntityWithHashKeyAndRangeKey.class, "RangeTest");
		query.range().between(TOP_RANGE_ID, BOTTOM_RANGE_ID);
		Assert.assertTrue(query.count() == 3);
		
		// test set op
		UpdateOperations<EntityWithHashKeyAndRangeKey> ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.set("string", "something new");
		Iterable<UpdateResult<EntityWithHashKeyAndRangeKey>> updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		List<EntityWithHashKeyAndRangeKey> listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getString().equals("something new"));
		}
		
		// test inc
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.inc("number");
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getNumber() == 1);
		}

		
		// test inc+3
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.inc("number", 3);
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getNumber() == 4);
		}

		
		// test inc-2
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.inc("number", -2);
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getNumber() == 2);
		}
		
		// test dec
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.dec("number");
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read.getNumber() == 1);
		}

		
		// test remove
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.remove("set", "B");
		updateResults= phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 2);
			Assert.assertTrue(read.getSet().contains("B") == false);
		}

		// test add
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.add("set", "D");
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 3);
			Assert.assertTrue(read.getSet().contains("D") == true);
		}

		// test addAll
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.addAll("set", Arrays.asList("E", "F"));
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 5);
			Assert.assertTrue(read.getSet().contains("E") == true);
			Assert.assertTrue(read.getSet().contains("F") == true);
		}
		
		// test removeAll
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.removeAll("set", Arrays.asList("A", "C"));
		updateResults= phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getSet().size() == 3);
			Assert.assertTrue(read.getSet().contains("A") == false);
			Assert.assertTrue(read.getSet().contains("C") == false);
		}
		
		// test unset
		ops = phoebe.createUpdateOperations(EntityWithHashKeyAndRangeKey.class);
		ops.unset("string");
		updateResults = phoebe.getDatastore().update(query, ops);
		checkUpdateResults(updateResults);
		listRead = query.asList();
		Assert.assertTrue(listRead.size() == 3);
		for (EntityWithHashKeyAndRangeKey read: listRead) {
			Assert.assertTrue(read != null);
			Assert.assertTrue(read.getString() == null);
		}
		
	}
	
	private void checkUpdateResults(Iterable<UpdateResult<EntityWithHashKeyAndRangeKey>> updateResults) {
		int count = 0;
		for (UpdateResult<EntityWithHashKeyAndRangeKey> updateResult : updateResults) {
			count++;
			Assert.assertTrue(updateResult.getException() == null);
			Assert.assertTrue(updateResult.isUpdated() == true);
		}
		Assert.assertTrue(count == 3);
	}


}
