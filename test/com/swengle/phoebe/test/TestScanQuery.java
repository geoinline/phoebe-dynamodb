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
import com.swengle.phoebe.test.model.ObjectWithHashKeyAndRangeKey;

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
		ObjectWithHashKeyAndRangeKey obj = new ObjectWithHashKeyAndRangeKey();
		obj.setId("FilterTest");
		obj.setRangeId(topObjectId);
		obj.setFoo("foobar");
		obj.setBar(topObjectId);
		phoebe.getDatastore().put(obj);
		
		// object 2
		obj.setRangeId(middleObjectId);
		obj.setBar(middleObjectId);
		phoebe.getDatastore().put(obj);
		
		// object 3
		obj.setRangeId(bottomObjectId);
		obj.setBar(bottomObjectId);
		phoebe.getDatastore().put(obj);
		
		// object 4
		obj.setRangeId(new ObjectId().toString());
		obj.setBar(null);
		Set<String> set = new HashSet<String>();
		set.add("A");
		set.add("B");
		set.add("C");
		obj.setSet(set);
		phoebe.getDatastore().put(obj);
		
		// object 5
		obj.setRangeId(new ObjectId().toString());
		obj.setBar(null);
		obj.getSet().remove("B");
		phoebe.getDatastore().put(obj);
		
		ScanQuery<ObjectWithHashKeyAndRangeKey> query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		Assert.assertTrue(query.count() == 5);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.withExclusiveStartKey().equal("FilterTest", middleObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").greaterThan(topObjectId);
		Assert.assertTrue(query.count() == 2);

		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").greaterThanOrEq(topObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").lessThan(bottomObjectId);
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").lessThanOrEq(bottomObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").equal(bottomObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").notEqual(bottomObjectId);
		Assert.assertTrue(query.count() == 4);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").between(topObjectId, bottomObjectId);
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").equal(topObjectId).field("rangeId").equal(topObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").equal(topObjectId).field("rangeId").greaterThan(topObjectId);
		Assert.assertTrue(query.count() == 0);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").equal(topObjectId).field("rangeId").greaterThanOrEq(topObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").isNull();
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").isNotNull();
		Assert.assertTrue(query.count() == 3);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").in(Arrays.asList(topObjectId, middleObjectId));
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").contains(topObjectId);
		Assert.assertTrue(query.count() == 1);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("bar").notContains(topObjectId);
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("set").contains("A");
		Assert.assertTrue(query.count() == 2);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.field("set").notContains("B");
		Assert.assertTrue(query.count() == 1);
	}

	@Test
	@Ignore("test takes a lot of provisioned thru-put")
	public void testBigEntities() {
		List<ObjectWithHashKeyAndRangeKey> listToSave = new ArrayList<ObjectWithHashKeyAndRangeKey>();
		for (int j=0; j<20; j++) {
			ObjectWithHashKeyAndRangeKey entity = new ObjectWithHashKeyAndRangeKey();
			entity.setId("ReallyBigEntity");
			entity.setRangeId(String.valueOf(j));
			StringBuffer sb = new StringBuffer();
			int count = 64000/26;
			for (int i=0; i<count; i++) {
				sb.append("abcdefghijklmnopqrstuvwxyz");
			}
			entity.setFoo(sb.toString());
			listToSave.add(entity);
		}
		phoebe.getDatastore().put(listToSave);
		
		ScanQuery<ObjectWithHashKeyAndRangeKey> query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		Assert.assertTrue(query.count() == 20);
		
		query = phoebe.createScanQuery(ObjectWithHashKeyAndRangeKey.class);
		query.limit(19);
		Assert.assertTrue(query.count() == 19);
	}
	
}
