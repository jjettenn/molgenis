package org.molgenis.data.meta;

import static java.lang.String.format;
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
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.Query;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCapability;
import org.molgenis.data.SystemEntityMetaDataRegistry;

public class PackageRepositoryDecorator implements Repository
{
	private final Repository decoratedRepo;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;

	public PackageRepositoryDecorator(Repository decoratedRepo,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry)
	{
		this.decoratedRepo = requireNonNull(decoratedRepo);
		this.systemEntityMetaDataRegistry = requireNonNull(systemEntityMetaDataRegistry);
	}

	@Override
	public Set<RepositoryCapability> getCapabilities()
	{
		return decoratedRepo.getCapabilities();
	}

	@Override
	public void close() throws IOException
	{
		decoratedRepo.close();
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
	public Iterator<Entity> iterator()
	{
		return decoratedRepo.iterator();
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
		validateUpdateAllowed(entity);
		decoratedRepo.update(entity);
	}

	@Override
	public void update(Stream<? extends Entity> entities)
	{
		decoratedRepo.update(entities.filter(entity -> {
			validateUpdateAllowed(entity);
			return true;
		}));
	}

	@Override
	public void delete(Entity entity)
	{
		validateDeleteAllowed(entity);
		decoratedRepo.delete(entity);
	}

	@Override
	public void delete(Stream<? extends Entity> entities)
	{
		decoratedRepo.delete(entities.filter(entity -> {
			validateDeleteAllowed(entity);
			return true;
		}));
	}

	@Override
	public void deleteById(Object id)
	{
		validateDeleteAllowed(findOne(id));
		decoratedRepo.deleteById(id);
	}

	@Override
	public void deleteById(Stream<Object> ids)
	{
		decoratedRepo.deleteById(ids.filter(id -> {
			validateDeleteAllowed(findOne(id));
			return true;
		}));
	}

	@Override
	public void deleteAll()
	{
		iterator().forEachRemaining(this::validateDeleteAllowed);
		decoratedRepo.deleteAll();
	}

	@Override
	public void add(Entity entity)
	{
		validateAddAllowed(entity);
		decoratedRepo.add(entity);
	}

	@Override
	public Integer add(Stream<? extends Entity> entities)
	{
		return decoratedRepo.add(entities.filter(entity -> {
			validateAddAllowed(entity);
			return true;
		}));
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

	private void validateAddAllowed(Entity entity)
	{
		String packageName = entity.getString(PackageMetaData.FULL_NAME);
		if (systemEntityMetaDataRegistry.isSystemPackage(packageName))
		{
			throw new MolgenisDataException(format("Adding system package [%s] is not allowed", packageName));
		}
	}

	private void validateUpdateAllowed(Entity entity)
	{
		String packageName = entity.getString(PackageMetaData.FULL_NAME);
		if (systemEntityMetaDataRegistry.isSystemPackage(packageName))
		{
			throw new MolgenisDataException(format("Updating system package [%s] is not allowed", packageName));
		}
	}

	private void validateDeleteAllowed(Entity entity)
	{
		String packageName = entity.getString(PackageMetaData.FULL_NAME);
		if (systemEntityMetaDataRegistry.isSystemPackage(packageName))
		{
			throw new MolgenisDataException(format("Deleting system package [%s] is not allowed", packageName));
		}
	}
}
