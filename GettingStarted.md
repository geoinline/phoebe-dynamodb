# A quick introduction to Phoebe. #

## Annotations ##
To make things as compatible with Amazons AWS library annotations remain the same as shown <a href='http://docs.amazonwebservices.com/amazondynamodb/latest/developerguide/JavaSDKHighLevel.html#JavaDeclarativeTagsList'>here</a>. We could have chosen to implement our own annotations (it would have been preferable to annotate the member fields rather that the getters) but we felt that it was better to be consistent at this time.

## Starting things up ##
As a best practice, your applications should create one Phoebe and reuse it between threads.

```
Phoebe phoebe = new Phoebe(awsCredentials);
```


## Keys ##
All entities stored in DynamoDB have either the equivalent of a PrimaryKey (known as a HashKey) (which can be a string or a number) or a CompositeKey (a combination of a HashKey and a RangeKey).
The native AWS Key class is simple and untyped. Phoebe provides a generified `EntityKey` that carries type information:


```
EntityKey<Car> keyWithHash = EntityKey.create(Car.class, "Ford");
EntityKey<Car> keyWithRange = EntityKey.create(Car.class, "Ford", "Mustang");
```

These `EntityKey`s are the key (pun-intended) to all operations in Phoebe.

## Phoebe's datastores ##
Phoebe provides four types of datastores to perform your operations:

```
phoebe.getDatastore()
phoebe.getAsyncDatastore()
phoebe.getConsistentReadDatastore()
phoebe.getConsistentReadAsyncDatastore()
```

DynamoDB provides two types of consistency models when reading data. Using either `getConsistentReadDatastore()` or `getConsistentReadAsyncDatastore()` will always ensure you are reading the latest data (at somewhat higher cost). Alternatively if it's not important that you are reading the latest up to date data you can use one of the other non-consistent datastores.

Under the hood Phoebe always uses the asynchronous datastores to perform all operations. The non-asynchronous datastores just provide a wrapper around them and return the result rather than a `FutureResult`. The asynchronous datastores return a `FutureResult` which is much like a Java `Future` but with sane exception handling behavior.

## Enough already, how do I put, get, update and delete my entities ##
### Put and Insert ###
Phoebe provides two methods for savings your entities. These methods are similar but have some slight behavior differences. So to simply put an entity we do this:

```
Car car = new Car();
car.setId("mini");
car.setColor("red");
phoebe.getDatastore().put(car);
```

Using `put` means that if a car already exists with the same key then it will be updated with the new values. It will also comply with the AWS Java library's concept of `@DynamoDBVersionAttribute`.

If however we do the following:

```
phoebe.getDatastore().insert(car);
```

Then if a car already exists with the same key a `DuplicateEntityException` will occur. Also `@DynamoDBVersionAttribute` is not adhered to (it's just simply ignore).

Of course there's always the need to save more than one entity at a time:
```
phoebe.getDatastore().insert(Arrays.asList(car1, car2, car3));
```

### Get (Read) ###
Now that we have saved entities in the datastore; how do we get them out? The answer is dead simple:

```
Car car = phoebe.getDatastore().get(Car.class, "mini");
```

Using an `EntityKey` we could have also done
```
Car car = phoebe.getDatastore().get(EntityKey.create(Car.class, "mini"));
```

Again we can read multiple entities
```
List<Car> cars = phoebe.getDatastore().get(Car.class, "mini", "porsche", "mustang");
```

There are many more ways to read entities (yeah auto-complete is your best friend).

### Delete ###
There are a lots of ways to delete an entity. It's probably best to show the various methods:

```
	/** Deletes the given entities by hashKey and rangeKey **/
	<T> Iterable<DeleteResult<T>> delete(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, Iterable<String> hashKeyRangeKeys);
	/** Deletes the given entity by hashKey and rangeKey **/
	<T> Void delete(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String hashKeyRangeKey);
	/** Deletes the given entities by hashKey and rangeKey **/
	<T> Iterable<DeleteResult<T>> delete(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String... hashKeyRangeKeys);
	/** Deletes the given entities by hashKey **/
	<T> Iterable<DeleteResult<T>> delete(Class<T> kindClass,
			Iterable<? extends Object> hashKeys);
	/** Deletes the given entity by hashKey **/
	<T> Void delete(Class<T> kindClass, Object hashKey);
	/** Deletes the given entities by hashKey **/
	<T> Iterable<DeleteResult<T>> delete(Class<T> kindClass,
			Object... hashKeys);
	/** Deletes the given entity by hashKey and rangeKey **/
	<T> Void delete(Class<T> kindClass, Object hashKey,
			Object rangeKey);
	/** Deletes the given entity by EntityKey **/
	<T> Void delete(EntityKey<T> entityKey);
	/** Deletes the given entities by EntityKey **/
	<T> Iterable<DeleteResult<T>> delete(
			Iterable<EntityKey<T>> entityKeys);
	/** Deletes the given entities based on the query **/
	<T> Iterable<DeleteResult<T>> delete(
			Query<T> query);
	/** Deletes the given entity (by EntityKey) **/
	<T> Void delete(T entity);
```

You probably looked at that and said what the heck is a `Query<T>` and what is a `HashKeyRangeKeyResolver`. More on that later.

### Update ###
To update an entity we make use of `UpdateOperations`. Again it is probably easiest to show the interfaces:

```
	/** adds the val to a set field **/
	UpdateOperations<T> add(String fieldName, Object val);
	/** adds the vals to a set field **/
	UpdateOperations<T> addAll(String fieldName, Iterable<?> vals);
	/** decrements the number field by 1 **/
	UpdateOperations<T> dec(String fieldName);
	/** increments the number field by 1 **/
	UpdateOperations<T> inc(String fieldName);
	/** increments the number field by value (negative values allowed) **/
	UpdateOperations<T> inc(String fieldName, Number value);
	/** removes val from the set field **/
	UpdateOperations<T> remove(String fieldName, Object val);
	/** removes vals from the set field **/
	UpdateOperations<T> removeAll(String fieldName, Iterable<?> vals);
	/** sets the field to val **/
	UpdateOperations<T> set(String fieldName, Object val);
	/** removes the field **/
	UpdateOperations<T> unset(String fieldName);
```

To create an `UpdateOperations` we simply do

```
UpdateOperations ops = phoebe.createUpdateOperations(Car.class);
ops.set("color", "orange");
```

Notice how we refer to the classes field name (color) and we don't need to know what the attributes name in the datastore is.

Then we use the datastore to update the correct entity:

```
phoebe.getDatastore().update(EntityKey.create(Car.class, "mini"), ops);
```

Again we can update multiple entities using:

```
<T> Iterable<UpdateResult<T>> update(Iterable<EntityKey<T>> entityKeys, UpdateOperations<T> ops);
```


### Query ###
There are two types of queries Phoebe can perform (more will follow).

**ScanQuery**

```
ScanQuery<Car> scanQuery = phoebe.createScanQuery(Car.class);
scanQuery.limit(10);
scanQuery.field("color").equal("red");
List<Car> redCars = scanQuery.asList();
```

A `ScanQuery` is an expensive query and should probably only be done on tables that you know are small.

**RangeQuery**

Fairly similar to a `ScanQuery`

```
RangeQuery<CarModel> rangeQuery = phoebe.createRangeQuery(CarModel.class, "Ford");
rangeQuery.limit(10);
rangeQuery.range().greaterThan("1995");
List<CarModel> carsSince1995 = rangeQuery.asList();
```

In this case we are going to look for all `CarModel` entities with a hashKey of "Ford" and a rangeKey greater than 1995. So if we were to store a `CarModel` entity for all cars made by Ford with the year they were released in the rangeKey (probably more something like YEAR-MODEL#) we can easily find those models using the rangeKey to filter by year of release.


### HashKeyRangeKeyResolver ###

So what the heck is this `HashKeyRangeKeyResolver` thing? It can be cumbersome to always need to pass two parts of a composite key (an entity with a hashKey and rangeKey). If you are developing a web app you would probably want to represent the key as a single string. The `HashKeyRangeKeyResolver` provides the following interface:

```
	/**
	 * Should return a string array of length 1 for just a hashKey, a length of 2 for a hashKey and rangeKey
	 * string[0] should be the string representation of the hashKey
	 * string[1] (if necessary) should be the string representation of the rangeKey
	 */
	String[] split(String hashKeyRangeKey);
	/**
	 * Combines the hashKey and rangeKey into a single string
	 * This string should be usable in the split() method
	 */
	String join(Object hashKey, Object rangeKey);
```

Pheobe includes an implementation for a simple delimited type resolver called `DelimiterHashKeyRangeKeyResolver`.

So we could use this `DelimiterHashKeyRangeKeyResolver` as follows:

```
String carModelId = "Ford:1995-0001";
DelimiterHashKeyRangeKeyResolver resolver = new DelimiterHashKeyRangeKeyResolver(":");
EntityKey<CarModel> key = EntityKey.create(CarModel.class, resolver, carModelId);
```

This is a simple example but the main strength comes from the ability to do bulk operations with the datastore:

```
List<CarModel> carModels = phoebe.getDatastore().get(CarModel.class, resolver, "Ford:1995-0001", "Ford:1995-0002", "Ford:1995-0003");
```


### Enjoy ###
This ends the quick introduction to Phoebe. There is a lot more to come and the documentation will improve as the API settles down. There is probably enough to get started though and don't be afraid to ask questions.