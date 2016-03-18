package org.molgenis.data.cache;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.molgenis.data.AggregateQuery;
import org.molgenis.data.AggregateResult;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityListener;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Fetch;
import org.molgenis.data.Query;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCapability;

public class TransactionEntityCacheDecorator implements Repository
{
	private final Repository decoratedRepo;
	private final TransactionEntityCache transactionEntityCache;

	public TransactionEntityCacheDecorator(Repository decoratedRepo, TransactionEntityCache transactionEntityCache)
	{
		this.decoratedRepo = requireNonNull(decoratedRepo);
		this.transactionEntityCache = requireNonNull(transactionEntityCache);
	}

	@Override
	public Stream<Entity> stream(Fetch fetch)
	{
		return decoratedRepo.stream(fetch);
	}

	@Override
	public void close() throws IOException
	{
		decoratedRepo.close();
	}

	@Override
	public Set<RepositoryCapability> getCapabilities()
	{
		return decoratedRepo.getCapabilities();
	}

	@Override
	public String getName()
	{
		return decoratedRepo.getName();
	}

	@Override
	public EntityMetaData getEntityMetaData()
	{
		return decoratedRepo.getEntityMetaData();
	}

	@Override
	public Iterator<Entity> iterator()
	{
		return decoratedRepo.iterator();
	}

	@Override
	public long count()
	{
		return decoratedRepo.count();
	}

	@Override
	public Query query()
	{
		return decoratedRepo.query();
	}

	@Override
	public long count(Query q)
	{
		return decoratedRepo.count(q);
	}

	@Override
	public Stream<Entity> findAll(Query q)
	{
		return decoratedRepo.findAll(q);
	}

	@Override
	public Entity findOne(Query q)
	{
		return decoratedRepo.findOne(q);
	}

	@Override
	public Entity findOne(Object id)
	{
		Entity entity = transactionEntityCache.cacheGet(getName(), id);
		if (entity == null)
		{
			entity = decoratedRepo.findOne(id);
			if (entity != null)
			{
				transactionEntityCache.cachePut(getName(), entity);
			}
		}
		return entity;
	}

	@Override
	public Entity findOne(Object id, Fetch fetch)
	{
		return decoratedRepo.findOne(id, fetch);
	}

	@Override
	public Stream<Entity> findAll(Stream<Object> ids)
	{
		return decoratedRepo.findAll(ids);
	}

	@Override
	public Stream<Entity> findAll(Stream<Object> ids, Fetch fetch)
	{
		return decoratedRepo.findAll(ids, fetch);
	}

	@Override
	public AggregateResult aggregate(AggregateQuery aggregateQuery)
	{
		return decoratedRepo.aggregate(aggregateQuery);
	}

	@Override
	public void update(Entity entity)
	{
		transactionEntityCache.cacheEvict(getName(), entity.getIdValue());
		decoratedRepo.update(entity);
	}

	@Override
	public void update(Stream<? extends Entity> entities)
	{
		entities = entities.filter(entity -> {
			transactionEntityCache.cacheEvict(getName(), entity.getIdValue());
			return true;
		});
		decoratedRepo.update(entities);
	}

	@Override
	public void delete(Entity entity)
	{
		transactionEntityCache.cacheEvict(getName(), entity.getIdValue());
		decoratedRepo.delete(entity);
	}

	@Override
	public void delete(Stream<? extends Entity> entities)
	{
		entities = entities.filter(entity -> {
			transactionEntityCache.cacheEvict(getName(), entity.getIdValue());
			return true;
		});
		decoratedRepo.delete(entities);
	}

	@Override
	public void deleteById(Object id)
	{
		transactionEntityCache.cacheEvict(getName(), id);
		decoratedRepo.deleteById(id);
	}

	@Override
	public void deleteById(Stream<Object> ids)
	{
		ids = ids.filter(id -> {
			transactionEntityCache.cacheEvict(getName(), id);
			return true;
		});
		decoratedRepo.deleteById(ids);
	}

	@Override
	public void deleteAll()
	{
		transactionEntityCache.cacheEvictAll(getName());
		decoratedRepo.deleteAll();
	}

	@Override
	public void add(Entity entity)
	{
		decoratedRepo.add(entity);
	}

	@Override
	public Integer add(Stream<? extends Entity> entities)
	{
		return decoratedRepo.add(entities);
	}

	@Override
	public void flush()
	{
		decoratedRepo.flush();
	}

	@Override
	public void clearCache()
	{
		decoratedRepo.clearCache();
	}

	@Override
	public void create()
	{
		decoratedRepo.create();
	}

	@Override
	public void drop()
	{
		transactionEntityCache.cacheEvictAll(getName());
		decoratedRepo.drop();
	}

	@Override
	public void rebuildIndex()
	{
		decoratedRepo.rebuildIndex();
	}

	@Override
	public void addEntityListener(EntityListener entityListener)
	{
		decoratedRepo.addEntityListener(entityListener);
	}

	@Override
	public void removeEntityListener(EntityListener entityListener)
	{
		decoratedRepo.removeEntityListener(entityListener);
	}
}
