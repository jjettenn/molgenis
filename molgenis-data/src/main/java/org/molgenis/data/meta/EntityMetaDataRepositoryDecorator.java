package org.molgenis.data.meta;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.molgenis.util.SecurityDecoratorUtils.validatePermission;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.molgenis.data.AggregateQuery;
import org.molgenis.data.AggregateResult;
import org.molgenis.data.DataService;
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
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.support.DataServiceImpl;
import org.molgenis.security.core.Permission;
import org.molgenis.security.core.utils.SecurityUtils;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Sets;
import com.google.common.collect.TreeTraverser;

public class EntityMetaDataRepositoryDecorator implements Repository
{
	private final Repository decoratedRepo;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;
	private final DataService dataService;
	private final LanguageService languageService;

	public EntityMetaDataRepositoryDecorator(Repository decoratedRepo, DataService dataService,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry, LanguageService languageService)
	{
		this.decoratedRepo = requireNonNull(decoratedRepo);
		this.dataService = requireNonNull(dataService);
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

	@Transactional
	@Override
	public void update(Entity entity)
	{
		updateEntity(entity);
	}

	@Transactional
	@Override
	public void update(Stream<? extends Entity> entities)
	{
		entities.forEach(this::updateEntity);
	}

	@Transactional
	@Override
	public void delete(Entity entity)
	{
		deleteEntity(entity);
	}

	@Transactional
	@Override
	public void delete(Stream<? extends Entity> entities)
	{
		entities.forEach(this::deleteEntity);
	}

	@Transactional
	@Override
	public void deleteById(Object id)
	{
		Entity entity = findOne(id);
		if (entity == null)
		{
			throw new UnknownEntityException(format("Unknown entity [%s] with id [%s]", getName(), id.toString()));
		}
		deleteEntity(entity);
	}

	@Transactional
	@Override
	public void deleteById(Stream<Object> ids)
	{
		findAll(ids).forEach(this::deleteEntity);
	}

	@Transactional
	@Override
	public void deleteAll()
	{
		iterator().forEachRemaining(this::deleteEntity);
	}

	@Transactional
	@Override
	public void add(Entity entity)
	{
		addEntity(entity);
	}

	@Transactional
	@Override
	public Integer add(Stream<? extends Entity> entities)
	{
		AtomicInteger count = new AtomicInteger();
		entities.filter(entity -> {
			count.incrementAndGet();
			return true;
		}).forEach(this::addEntity);
		return count.get();
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

	private void addEntity(Entity entityEntity)
	{
		validateAddAllowed(entityEntity);

		// add row to entities table
		decoratedRepo.add(entityEntity);

		// create entity table
		EntityMetaData entityMeta = MetaUtils.toEntityMeta(entityEntity, this, languageService.getLanguageCodes());
		if (!entityMeta.isAbstract())
		{
			Repository entityRepo = dataService.getMeta().getBackend(entityMeta.getBackend()).addEntityMeta(entityMeta);
			((DataServiceImpl) dataService).addRepository(entityRepo); // FIXME remove cast
		}
	}

	private void validateAddAllowed(Entity entity)
	{
		validatePermission(entity.getString(EntityMetaDataMetaData.FULL_NAME), Permission.WRITEMETA);

		Entity existingEntity = findOne(entity.getIdValue(), new Fetch().field(EntityMetaDataMetaData.FULL_NAME));
		if (existingEntity != null)
		{
			throw new MolgenisDataException(format("Adding existing entity [%s] is not allowed",
					entity.getString(EntityMetaDataMetaData.FULL_NAME)));
		}

		EntityMetaData entityMeta = MetaUtils.toEntityMeta(entity, this, languageService.getLanguageCodes());
		MetaValidationUtils.validateEntityMetaData(entityMeta);
	}

	private void updateEntity(Entity entityEntity)
	{
		validateUpdateAllowed(entityEntity);

		Entity existingEntityEntity = findOne(entityEntity.getIdValue());
		if (existingEntityEntity == null)
		{
			throw new UnknownEntityException(format("Unknown entity [%s] with id [%s]", getName(), entityEntity.getIdValue().toString()));
		}
		updateEntityAttributes(entityEntity, existingEntityEntity);

		decoratedRepo.update(entityEntity);
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
		validatePermission(entity.getString(EntityMetaDataMetaData.FULL_NAME), Permission.WRITEMETA);

		String entityName = entity.getString(EntityMetaDataMetaData.FULL_NAME);
		EntityMetaData entityMetaData = systemEntityMetaDataRegistry.getSystemEntity(entityName);
		if (entityMetaData != null && !MetaUtils.equals(entity, entityMetaData, dataService.getMeta()))
		{
			throw new MolgenisDataException(format("Updating system entity [%s] is not allowed", entityName));
		}

		MetaValidationUtils
				.validateEntityMetaData(MetaUtils.toEntityMeta(entity, this, languageService.getLanguageCodes()));
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
			RepositoryCollection repoCollection = dataService.getMeta().getBackend(backend);
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

	private void deleteEntity(Entity entityEntity)
	{
		validateDeleteAllowed(entityEntity);

		// delete row from entities table
		decoratedRepo.delete(entityEntity);

		// delete rows from attributes table
		deleteEntityAttributes(entityEntity);

		// delete rows from tags table
		deleteEntityTags(entityEntity);

		// delete entity table
		deleteEntityInstances(entityEntity);

		// delete entity permissions
		deleteEntityPermissions(entityEntity);
	}

	private void validateDeleteAllowed(Entity entity)
	{
		validatePermission(entity.getString(EntityMetaDataMetaData.FULL_NAME), Permission.WRITEMETA);

		Boolean isSystem = entity.getBoolean(EntityMetaDataMetaData.SYSTEM);
		if (isSystem == null || isSystem.booleanValue())
		{
			throw new MolgenisDataException(format("Deleting system entity [%s] is not allowed",
					entity.getString(EntityMetaDataMetaData.FULL_NAME)));
		}
	}

	private void deleteEntityAttributes(Entity entityEntity)
	{
		Iterable<Entity> rootAttrEntities = entityEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES);
		Stream<Entity> attrEntities = StreamSupport.stream(rootAttrEntities.spliterator(), false)
				.flatMap(attrEntity -> StreamSupport.stream(new TreeTraverser<Entity>()
				{
					@Override
					public Iterable<Entity> children(Entity attrEntity)
					{
						return attrEntity.getEntities(AttributeMetaDataMetaData.PARTS);
					}
				}.postOrderTraversal(attrEntity).spliterator(), false));
		dataService.delete(AttributeMetaDataMetaData.ENTITY_NAME, attrEntities);
	}

	private void deleteEntityTags(Entity entityEntity)
	{
		Iterable<Entity> tagEntities = entityEntity.getEntities(EntityMetaDataMetaData.TAGS);
		dataService.delete(TagMetaData.ENTITY_NAME, StreamSupport.stream(tagEntities.spliterator(), false));
	}

	private void deleteEntityInstances(Entity entityEntity)
	{
		String entityName = entityEntity.getString(EntityMetaDataMetaData.FULL_NAME);
		String backend = entityEntity.getString(EntityMetaDataMetaData.BACKEND);
		((ManageableRepositoryCollection) dataService.getMeta().getBackend(backend)).deleteEntityMeta(entityName);
	}

	private void deleteEntityPermissions(Entity entityEntity)
	{
		String entityName = entityEntity.getString(EntityMetaDataMetaData.FULL_NAME);
		List<String> authorities = SecurityUtils.getEntityAuthorities(entityName);

		// User permissions
		if (dataService.hasRepository("UserAuthority"))
		{
			Stream<Entity> userPermissions = dataService.query("UserAuthority").in("role", authorities).findAll();
			dataService.delete("UserAuthority", userPermissions);
		}

		// Group permissions
		if (dataService.hasRepository("GroupAuthority"))
		{
			Stream<Entity> groupPermissions = dataService.query("GroupAuthority").in("role", authorities).findAll();
			dataService.delete("GroupAuthority", groupPermissions);
		}
	}
}
