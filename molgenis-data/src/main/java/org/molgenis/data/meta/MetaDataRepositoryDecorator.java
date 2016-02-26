package org.molgenis.data.meta;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.molgenis.data.RepositoryCapability.MANAGABLE;

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

/**
 * Repository decorator for entities, attributes and packages repositories.
 * 
 * Removes the WRITABLE and MANAGEABLE capabilities, because the user must not directly edit these repos but use the
 * MetaDataServices
 */
public class MetaDataRepositoryDecorator implements Repository
{
	private final Repository decoratedRepo;
	private final MetaDataService metaDataService;

	public MetaDataRepositoryDecorator(Repository decoratedRepo, MetaDataService metaDataService)
	{
		this.decoratedRepo = requireNonNull(decoratedRepo);
		this.metaDataService = requireNonNull(metaDataService);
	}

	@Override
	public Iterator<Entity> iterator()
	{
		return decoratedRepo.iterator();
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
		Stream<RepositoryCapability> capabilities = decoratedRepo.getCapabilities().stream();
		return capabilities.filter(capability -> !capability.equals(MANAGABLE)).collect(toSet());
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
		return decoratedRepo.findOne(id);
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
		decoratedRepo.update(entity);
		metaDataService.refreshCaches();
	}

	@Override
	public void update(Stream<? extends Entity> entities)
	{
		decoratedRepo.update(entities);
		metaDataService.refreshCaches();
	}

	@Override
	public void delete(Entity entity)
	{
		decoratedRepo.delete(entity);
		metaDataService.refreshCaches();
	}

	@Override
	public void delete(Stream<? extends Entity> entities)
	{
		decoratedRepo.delete(entities);
		metaDataService.refreshCaches();
	}

	@Override
	public void deleteById(Object id)
	{
		decoratedRepo.deleteById(id);
		metaDataService.refreshCaches();
	}

	@Override
	public void deleteById(Stream<Object> ids)
	{
		decoratedRepo.deleteById(ids);
		metaDataService.refreshCaches();
	}

	@Override
	public void deleteAll()
	{
		decoratedRepo.deleteAll();
		metaDataService.refreshCaches();
	}

	@Override
	public void add(Entity entity)
	{
		decoratedRepo.add(entity);
		metaDataService.refreshCaches();
	}

	@Override
	public Integer add(Stream<? extends Entity> entities)
	{
		Integer count = decoratedRepo.add(entities);
		metaDataService.refreshCaches();
		return count;
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
		throw new UnsupportedOperationException(format("Repository [%s] is not %s", getName(), MANAGABLE.toString()));
	}

	@Override
	public void drop()
	{
		throw new UnsupportedOperationException(format("Repository [%s] is not %s", getName(), MANAGABLE.toString()));
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
