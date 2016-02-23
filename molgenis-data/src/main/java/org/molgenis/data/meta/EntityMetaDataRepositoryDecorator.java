package org.molgenis.data.meta;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.molgenis.data.AggregateQuery;
import org.molgenis.data.AggregateResult;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityListener;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Fetch;
import org.molgenis.data.ManageableRepositoryCollection;
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.Query;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCapability;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.i18n.LanguageService;

import com.google.common.collect.Sets;

public class EntityMetaDataRepositoryDecorator implements Repository
{
	private final Repository decoratedRepo;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;
	private final MetaDataService metaDataService;
	private final LanguageService languageService;

	public EntityMetaDataRepositoryDecorator(Repository decoratedRepo, MetaDataService metaDataService,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry, LanguageService languageService)
	{
		this.decoratedRepo = requireNonNull(decoratedRepo);
		this.metaDataService = requireNonNull(metaDataService);
		this.systemEntityMetaDataRegistry = requireNonNull(systemEntityMetaDataRegistry);
		this.languageService = requireNonNull(languageService);
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
		updateEntityAttributes(entity, findOne(entity.getIdValue()));
		decoratedRepo.update(entity);
	}

	@Override
	public void update(Stream<? extends Entity> entities)
	{
		decoratedRepo.update(entities.filter(entity -> {
			validateUpdateAllowed(entity);
			updateEntityAttributes(entity, findOne(entity.getIdValue()));
			return true;
		}));
	}

	@Override
	public void delete(Entity entity)
	{
		validateDeleteAllowed(entity);
		decoratedRepo.delete(entity);
		// FIXME delete table
	}

	@Override
	public void delete(Stream<? extends Entity> entities)
	{
		decoratedRepo.delete(entities.filter(entity -> {
			validateDeleteAllowed(entity);
			return true;
		}));
		// FIXME delete table
	}

	@Override
	public void deleteById(Object id)
	{
		validateDeleteAllowed(findOne(id));
		decoratedRepo.deleteById(id);
		// FIXME delete table
	}

	@Override
	public void deleteById(Stream<Object> ids)
	{
		decoratedRepo.deleteById(ids.filter(id -> {
			validateDeleteAllowed(findOne(id));
			return true;
		}));
		// FIXME delete table
	}

	@Override
	public void deleteAll()
	{
		iterator().forEachRemaining(this::validateDeleteAllowed);
		decoratedRepo.deleteAll();
		// FIXME delete table
	}

	@Override
	public void add(Entity entity)
	{
		validateAddAllowed(entity);
		decoratedRepo.add(entity);
		// FIXME create table
	}

	@Override
	public Integer add(Stream<? extends Entity> entities)
	{
		return decoratedRepo.add(entities.filter(entity -> {
			validateAddAllowed(entity);
			return true;
		}));
		// FIXME create table
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
		Entity existingEntity = findOne(entity.getIdValue(), new Fetch().field(EntityMetaDataMetaData.FULL_NAME));
		if (existingEntity != null)
		{
			throw new MolgenisDataException(format("Adding existing entity [%s] is not allowed",
					entity.getString(EntityMetaDataMetaData.FULL_NAME)));
		}
	}

	/**
	 * Updating entity meta data is allowed for non-system entities. For system entities updating entity meta data is
	 * only allowed if the meta data defined in Java differs from the meta data stored in the database (in other words
	 * the Java code was updated).
	 * 
	 * @param entity
	 */
	private void validateUpdateAllowed(Entity entity)
	{
		String entityName = entity.getString(EntityMetaDataMetaData.FULL_NAME);
		EntityMetaData entityMetaData = systemEntityMetaDataRegistry.getSystemEntity(entityName);
		if (entityMetaData != null && !MetaUtils.equals(entity, entityMetaData, metaDataService))
		{
			throw new MolgenisDataException(format("Updating system entity [%s] is not allowed", entityName));
		}
	}

	private void validateDeleteAllowed(Entity entity)
	{
		Boolean isSystem = entity.getBoolean(EntityMetaDataMetaData.SYSTEM);
		if (isSystem == null || isSystem.booleanValue())
		{
			throw new MolgenisDataException(format("Deleting system entity [%s] is not allowed",
					entity.getString(EntityMetaDataMetaData.FULL_NAME)));
		}
	}

	private void updateEntityAttributes(Entity entity, Entity existingEntity)
	{
		Entity currentEntity = findOne(entity.getIdValue(), new Fetch().field(EntityMetaDataMetaData.FULL_NAME)
				.field(EntityMetaDataMetaData.ATTRIBUTES, new Fetch().field(AttributeMetaDataMetaData.NAME)));
		Map<String, Entity> currentAttrMap = StreamSupport
				.stream(currentEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES).spliterator(), false).collect(
						toMap(attrEntity -> attrEntity.getString(AttributeMetaDataMetaData.NAME), Function.identity()));
		Map<String, Entity> updateAttrMap = StreamSupport
				.stream(entity.getEntities(EntityMetaDataMetaData.ATTRIBUTES).spliterator(), false).collect(
						toMap(attrEntity -> attrEntity.getString(AttributeMetaDataMetaData.NAME), Function.identity()));

		Set<String> deletedAttrNames = Sets.difference(currentAttrMap.keySet(), updateAttrMap.keySet());
		Set<String> addedAttrNames = Sets.difference(updateAttrMap.keySet(), currentAttrMap.keySet());
		Set<String> existingAttrNames = Sets.intersection(currentAttrMap.keySet(), updateAttrMap.keySet());

		if (!deletedAttrNames.isEmpty() || !addedAttrNames.isEmpty() || !existingAttrNames.isEmpty())
		{
			String entityName = entity.getString(EntityMetaDataMetaData.FULL_NAME);
			String backend = entity.getString(EntityMetaDataMetaData.BACKEND);
			RepositoryCollection repoCollection = metaDataService.getBackend(backend);
			if (!(repoCollection instanceof ManageableRepositoryCollection))
			{
				throw new MolgenisDataException(format("Modifying attributes not allowed for entity [%s]", entityName));
			}
			ManageableRepositoryCollection manageableRepoCollection = (ManageableRepositoryCollection) repoCollection;

			if (!deletedAttrNames.isEmpty())
			{
				deletedAttrNames.forEach(deletedAttrName -> {
					manageableRepoCollection.deleteAttribute(entityName, deletedAttrName);
				});
			}

			if (!addedAttrNames.isEmpty())
			{
				Repository self = this;
				addedAttrNames.stream().map(updateAttrMap::get).forEach(addedAttrEntity -> {
					manageableRepoCollection.addAttribute(entityName,
							MetaUtils.toAttribute(addedAttrEntity, self, languageService.getLanguageCodes()));
				});
			}

			if (!existingAttrNames.isEmpty())
			{
				existingAttrNames.stream().filter(existingAttrName -> {
					Entity currentAttr = currentAttrMap.get(existingAttrName);
					Entity updatedAttr = updateAttrMap.get(existingAttrName);
					return false; // FIXME
					// return !MetaUtils.equals(entityMetaEntity, entityMeta, this);
				}).map(existingAttrName -> {
					throw new UnsupportedOperationException(format("Cannot update attribute(s) [%s] of entity [%s]",
							existingAttrNames.stream().collect(joining(",")), entityName));
				});

			}
		}
	}
}
