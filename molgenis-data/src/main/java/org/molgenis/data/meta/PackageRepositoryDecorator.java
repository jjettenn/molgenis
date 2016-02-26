package org.molgenis.data.meta;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.molgenis.util.SecurityDecoratorUtils.hasPermission;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.molgenis.data.AggregateQuery;
import org.molgenis.data.AggregateResult;
import org.molgenis.data.DataService;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityListener;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Fetch;
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.Query;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCapability;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.security.core.Permission;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.TreeTraverser;

public class PackageRepositoryDecorator implements Repository
{
	private final Repository decoratedRepo;
	private final DataService dataService;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;

	public PackageRepositoryDecorator(Repository decoratedRepo, DataService dataService,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry)
	{
		this.decoratedRepo = requireNonNull(decoratedRepo);
		this.dataService = requireNonNull(dataService);
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
	public Stream<Entity> stream(Fetch fetch)
	{
		return decoratedRepo.stream(fetch);
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

	@Transactional
	@Override
	public void delete(Entity entity)
	{
		deletePackage(entity);
	}

	@Transactional
	@Override
	public void delete(Stream<? extends Entity> entities)
	{
		entities.forEach(this::deletePackage);
	}

	@Override
	public void deleteById(Object id)
	{
		deletePackage(findOne(id));
	}

	@Override
	public void deleteById(Stream<Object> ids)
	{
		findAll(ids).forEach(this::deletePackage);
	}

	@Override
	public void deleteAll()
	{
		stream().forEach(this::deletePackage);
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
		Entity existingEntity = findOne(entity.getIdValue(), new Fetch().field(PackageMetaData.FULL_NAME));
		if (existingEntity != null)
		{
			throw new MolgenisDataException(format("Adding existing package [%s] is not allowed",
					entity.getString(EntityMetaDataMetaData.FULL_NAME)));
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

	private void deletePackage(Entity packageEntity)
	{
		validateDeleteAllowed(packageEntity);

		// recursively delete sub packages
		getPackageTreeTraversal(packageEntity).forEach(this::deletePackageAndContents);
	}

	private void deletePackageAndContents(Entity packageEntity)
	{
		// delete entities in package
		Repository entityRepo = dataService.getRepository(EntityMetaDataMetaData.ENTITY_NAME);
		Stream<Entity> entityEntities = entityRepo.query().eq(EntityMetaDataMetaData.PACKAGE, packageEntity).findAll();
		entityRepo.delete(entityEntities); // TODO dependency resolving

		// delete row from package table
		decoratedRepo.delete(packageEntity);
	}

	/**
	 * Deleting a package is allowed if:
	 * <ul>
	 * <li>This package and descending packages do not contain a system package</li>
	 * <li>This package and descending packages do not contain a system entity</li>
	 * <li>User has {@link org.molgenis.security.core.Permission#WRITEMETA} permission on all entities in this package
	 * and descending packages</li>
	 * <li>There are no dependencies to entities in this package and descendant packages from entities in other packages
	 * </li>
	 * </ul>
	 * 
	 * @param rootPackageEntity
	 */
	private void validateDeleteAllowed(Entity rootPackageEntity)
	{
		String rootPackageName = rootPackageEntity.getString(PackageMetaData.FULL_NAME);

		// 1. This package and descending packages do not contain a system package
		List<Entity> packageEntities = getPackageTreeTraversal(rootPackageEntity).collect(toList());

		packageEntities.forEach(packageEntity -> {
			String packageName = packageEntity.getString(PackageMetaData.FULL_NAME);
			if (systemEntityMetaDataRegistry.isSystemPackage(packageName))
			{
				if (packageName.equals(rootPackageName))
				{
					throw new MolgenisDataException(
							format("Deleting system package [%s] is not allowed", rootPackageName));
				}
				else
				{
					throw new MolgenisDataException(
							format("Deleting package [%s] is not allowed, because descendant package [%s] is a system package",
									rootPackageName, packageName));
				}
			}
		});

		Repository entityRepo = dataService.getRepository(EntityMetaDataMetaData.ENTITY_NAME);

		// 2. This package and descending packages do not contain a system entity
		List<Entity> systemEntityEntities = entityRepo.query().eq(EntityMetaDataMetaData.SYSTEM, true).and()
				.in(EntityMetaDataMetaData.PACKAGE, packageEntities).findAll().collect(toList());
		if (!systemEntityEntities.isEmpty())
		{
			throw new MolgenisDataException(
					format("Deleting package [%s] is not allowed, because package contains system entities [%s]",
							rootPackageName, systemEntityEntities.stream().map(Entity::getIdValue).map(Object::toString)
									.collect(joining(", "))));
		}

		// 3. User has write meta data permission on all entities in this package and descending packages
		entityRepo.query().in(EntityMetaDataMetaData.PACKAGE, packageEntities).findAll().forEach(entityEntity -> {
			String entityName = entityEntity.getString(EntityMetaDataMetaData.FULL_NAME);
			if (!hasPermission(entityName, Permission.WRITEMETA))
			{
				throw new MolgenisDataException(
						format("Deleting package [%s] is not allowed, because you don't have permission to delete entity [%s]",
								rootPackageName, entityName));
			}
		});

		// 4. There are no dependencies to entities in this package and descendant packages from entities in other
		// packages
		// TODO
	}

	private Stream<Entity> getPackageTreeTraversal(Entity packageEntity)
	{
		return StreamSupport.stream(new TreeTraverser<Entity>()
		{
			@Override
			public Iterable<Entity> children(Entity packageEntity)
			{
				return new Iterable<Entity>()
				{
					@Override
					public Iterator<Entity> iterator()
					{
						return query().eq(PackageMetaData.PARENT, packageEntity).findAll().iterator();
					}
				};
			}
		}.postOrderTraversal(packageEntity).spliterator(), false);
	}
}
