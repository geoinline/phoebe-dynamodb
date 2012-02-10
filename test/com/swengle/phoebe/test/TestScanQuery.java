/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.query.ScanQuery;
import com.swengle.phoebe.test.model.EntityWithHashKeyAndRangeKey;


/**
 * @author Administrator
 *
 */
public class TestScanQuery extends TestBase {
	private Phoebe phoebe = TestBase.PHOEBE;

	@Test
	public void testBasic() {
		emptyAll();
		String topObjectId = new ObjectId().toString();
		String middleObjectId = new ObjectId().toString();
		String bottomObjectId = new ObjectId().toString();
		
		// object 1
		EntityWithHashKeyAndRangeKey obj = new EntityWithHashKeyAndRangeKey();
		obj.setHashKey("ScanTest");
		obj.setRangeKey(topObjectId);
		obj.setString(topObjectId);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeKey(middleObjectId);
		obj.setString(middleObjectId);
		phoebe.getDatastore().put(obj);
		
		// object 3
		obj.setRangeKey(bottomObjectId);
		obj.setString(bottomObjectId);
		phoebe.getDatastore().put(obj);
		
		// object 4
		obj.setRangeKey(new ObjectId().toString());
		obj.setString(null);
		Set<String> set = new HashSet<String>();
		set.add("A");
		set.add("B");
		set.add("C");
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 5
		obj.setRangeKey(new ObjectId().toString());
		obj.setString(null);
		obj.getSet().remove("B");
		phoebe.getDatastore().put(obj);
		
		ScanQuery<EntityWithHashKeyAndRangeKey> query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		Assert.assertTrue(query.count() == 5);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.withExclusiveStartKey().equal("ScanTest", middleObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").greaterThan(topObjectId);
		Assert.assertTrue(query.count() == 2);

		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").greaterThanOrEq(topObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").lessThan(bottomObjectId);
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").lessThanOrEq(bottomObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").equal(bottomObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").notEqual(bottomObjectId);
		Assert.assertTrue(query.count() == 4);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").between(topObjectId, bottomObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").equal(topObjectId).field("rangeKey").equal(topObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").equal(topObjectId).field("rangeKey").greaterThan(topObjectId);
		Assert.assertTrue(query.count() == 0);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").equal(topObjectId).field("rangeKey").greaterThanOrEq(topObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").isNull();
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").isNotNull();
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").in(Arrays.asList(topObjectId, middleObjectId));
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").contains(topObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("string").notContains(topObjectId);
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("set").contains("A");
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.field("set").notContains("B");
		Assert.assertTrue(query.count() == 1);
	}

	@Test
	@Ignore("test takes a lot of provisioned thru-put")
	public void testBigEntities() {
		List<EntityWithHashKeyAndRangeKey> listToSave = new ArrayList<EntityWithHashKeyAndRangeKey>();
		for (int j=0; j<20; j++) {
			EntityWithHashKeyAndRangeKey entity = new EntityWithHashKeyAndRangeKey();
			entity.setHashKey("ReallyBigEntity");
			entity.setRangeKey(String.valueOf(j));
			StringBuffer sb = new StringBuffer();
			int count = 64000/26;
			for (int i=0; i<count; i++) {
				sb.append("abcdefghijklmnopqrstuvwxyz");
			}
			entity.setString(sb.toString());
			listToSave.add(entity);
		}
		phoebe.getDatastore().put(listToSave);
		
		ScanQuery<EntityWithHashKeyAndRangeKey> query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		Assert.assertTrue(query.count() == 20);
		
		query = phoebe.createScanQuery(EntityWithHashKeyAndRangeKey.class);
		query.limit(19);
		Assert.assertTrue(query.count() == 19);
	}
	
}
