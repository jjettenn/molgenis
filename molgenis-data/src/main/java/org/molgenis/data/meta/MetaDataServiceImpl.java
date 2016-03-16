package org.molgenis.data.meta;

import static com.google.common.collect.Lists.reverse;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.molgenis.security.core.utils.SecurityUtils.getCurrentUsername;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.molgenis.MolgenisFieldTypes.FieldTypeEnum;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Fetch;
import org.molgenis.data.IdGenerator;
import org.molgenis.data.Package;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.RepositoryCollectionRegistry;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.UnknownAttributeException;
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.i18n.LanguageMetaData;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.molgenis.fieldtypes.CompoundField;
import org.molgenis.util.DependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeTraverser;

/**
 * MetaData service. Administration of the {@link Package}, {@link EntityMetaData} and {@link AttributeMetaData} of the
 * metadata of the repositories.
 * 
 * TODO: This class smells. It started out as a simple administration but taken on a new role: to bootstrap the
 * repositories and orchestrate changes in metadata. There's a second, higher level, class in here that needs to be
 * refactored out. See also {@link MetaValidationUtils} which does some of this work now already.
 * 
 * <img src="http://yuml.me/041e5382.png" alt="Metadata entities" width="640"/>
 */
public class MetaDataServiceImpl implements MetaDataService
{
	private static final Logger LOG = LoggerFactory.getLogger(MetaDataServiceImpl.class);

	private RepositoryCollectionRegistry repositoryCollectionRegistry;
	private LanguageService languageService; // can be removed once entitymetadata extends entity
	private IdGenerator idGenerator; // can be removed once attribute identifiers are auto-generated
	private Package defaultPackage;
	private SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;

	private PackageMetaData packageMetaData;
	private EntityMetaDataMetaData entityMetaDataMetaData;
	private AttributeMetaDataMetaData attributeMetaDataMetaData;

	public MetaDataServiceImpl(RepositoryCollectionRegistry repositoryCollectionRegistry, IdGenerator idGenerator,
			DefaultPackage defaultPackage)
	{
		this.repositoryCollectionRegistry = requireNonNull(repositoryCollectionRegistry);
		this.idGenerator = requireNonNull(idGenerator);
		this.defaultPackage = requireNonNull(defaultPackage);
	}

	@Autowired
	public void setLanguageService(LanguageService languageService)
	{
		this.languageService = requireNonNull(languageService);
	}

	@Autowired
	public void setPackageMetaData(PackageMetaData packageMetaData)
	{
		this.packageMetaData = requireNonNull(packageMetaData);
	}

	@Autowired
	public void setEntityMetaDataMetaData(EntityMetaDataMetaData entityMetaDataMetaData)
	{
		this.entityMetaDataMetaData = requireNonNull(entityMetaDataMetaData);
	}

	@Autowired
	public void setAttributeMetaDataMetaData(AttributeMetaDataMetaData attributeMetaDataMetaData)
	{
		this.attributeMetaDataMetaData = requireNonNull(attributeMetaDataMetaData);
	}

	@Autowired
	public void setSystemEntityMetaDataRegistry(SystemEntityMetaDataRegistry systemEntityMetaDataRegistry)
	{
		this.systemEntityMetaDataRegistry = requireNonNull(systemEntityMetaDataRegistry);
	}

	@Override
	public RepositoryCollection getDefaultBackend()
	{
		return repositoryCollectionRegistry.getDefaultRepoCollection();
	}

	@Override
	public RepositoryCollection getMetaDataBackend()
	{
		return repositoryCollectionRegistry.getDefaultRepoCollection();
	}

	@Override
	public RepositoryCollection getBackend(String name)
	{
		return repositoryCollectionRegistry.getRepositoryCollection(name);
	}

	/**
	 * Removes entity meta data if it exists.
	 */
	@Override
	public void deleteEntityMeta(String entityName)
	{
		getEntityRepository().deleteById(entityName);
		refreshCaches();
		LOG.info("Repository [{}] deleted by user [{}]", entityName, getCurrentUsername());
	}

	@Transactional
	@Override
	public void delete(List<EntityMetaData> entities)
	{
		reverse(DependencyResolver.resolve(entities)).stream().map(EntityMetaData::getName)
				.forEach(this::deleteEntityMeta);
	}

	/**
	 * Removes an attribute from an entity.
	 */
	@Transactional
	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		EntityMetaData entityMeta = getEntityMetaData(entityName);
		if (entityMeta == null)
		{
			throw new UnknownEntityException(format("Unknown entity [%s]", entityName));
		}
		AttributeMetaData attr = entityMeta.getAttribute(attributeName);
		if (attr == null)
		{
			throw new UnknownAttributeException(
					format("Unknown attribute [%s] of entity [%s]", attributeName, entityName));
		}

		DefaultEntityMetaData updatedEntityMeta = new DefaultEntityMetaData(entityMeta);
		updatedEntityMeta.removeAttributeMetaData(attr);
		updateEntityMeta(updatedEntityMeta);
	}

	@Override
	public RepositoryCollection getBackend(EntityMetaData emd)
	{
		String backendName = emd.getBackend() == null ? getDefaultBackend().getName() : emd.getBackend();
		RepositoryCollection backend = getBackend(backendName);
		if (backend == null)
		{
			throw new RuntimeException("Unknown backend [" + backendName + "]");
		}

		return backend;
	}

	@Transactional
	@Override
	public synchronized Repository addEntityMeta(EntityMetaData entityMeta)
	{
		// if (dataService.hasRepository(entityMeta.getName()))
		// {
		// throw new MolgenisDataException(format("Entity [%s] already exists", entityMeta.getName()));
		// }

		LOG.info(format("Creating entity [%s]", entityMeta.getName()));

		// create attributes
		Stream<Entity> attrEntities = StreamSupport.stream(entityMeta.getOwnAttributes().spliterator(), false)
				.flatMap(this::getAttributesPostOrder).map(this::generateAttrIdentifier).map(MetaUtils::toEntity);
		getAttributeRepository().add(attrEntities);

		// TODO create tags

		Entity entityEntity = MetaUtils.toEntity(entityMeta);

		getEntityRepository().add(entityEntity);

		return !entityMeta.isAbstract() ? getRepository(entityMeta) : null;
	}

	// TODO where to delete entities for which java class was deleted?
	@Transactional
	@Override
	public List<AttributeMetaData> updateEntityMeta(EntityMetaData updatedEntityMeta)
	{
		Entity existingEntityMetaEntity = getEntityRepository().findOne(updatedEntityMeta.getName());
		if (existingEntityMetaEntity == null)
		{
			throw new UnknownEntityException(format("Unknown entity [%s]", updatedEntityMeta.getName()));
		}

		Entity updatedEntityMetaEntity = MetaUtils.toEntity(updatedEntityMeta);

		// workaround: if entity stored in repository has backend and entity meta has no backend
		String backend = updatedEntityMetaEntity.getString(EntityMetaDataMetaData.BACKEND);
		if (backend == null)
		{
			updatedEntityMetaEntity.set(EntityMetaDataMetaData.BACKEND, getDefaultBackend().getName());
		}
		// workaround: if entity stored in repository has package and entity meta has no package
		Entity packageEntity = updatedEntityMetaEntity.getEntity(EntityMetaDataMetaData.PACKAGE);
		if (packageEntity == null)
		{
			updatedEntityMetaEntity.set(EntityMetaDataMetaData.PACKAGE, MetaUtils.toEntity(DefaultPackage.INSTANCE));
		}

		// workaround: entity attribute in repository has identifier and entity meta attribute has no identifier
		Entity updatedIdAttr = updatedEntityMetaEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE);
		if (updatedIdAttr != null)
		{
			Entity existingIdAttr_ = existingEntityMetaEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE);
			if (existingIdAttr_ != null)
			{
				updatedIdAttr.set(AttributeMetaDataMetaData.IDENTIFIER,
						existingIdAttr_.getString(AttributeMetaDataMetaData.IDENTIFIER));
			}
		}
		Entity updatedLabelAttr = updatedEntityMetaEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE);
		if (updatedLabelAttr != null)
		{
			Entity existingLabelAttr_ = existingEntityMetaEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE);
			if (existingLabelAttr_ != null)
			{
				updatedLabelAttr.set(AttributeMetaDataMetaData.IDENTIFIER,
						existingLabelAttr_.getString(AttributeMetaDataMetaData.IDENTIFIER));
			}
		}

		Map<String, Entity> existingAttrNameAttrMap = createAttrIdentifierMap(existingEntityMetaEntity);
		injectAttrEntityIdentifiers(updatedEntityMetaEntity,
				existingAttrNameAttrMap.entrySet().stream().collect(toMap(entry -> entry.getKey(),
						entry -> entry.getValue().getString(AttributeMetaDataMetaData.IDENTIFIER))));
		Map<String, Entity> updatedAttrNameAttrMap = createAttrIdentifierMap(updatedEntityMetaEntity);

		// add new attributes
		Set<String> addedAttrNames = Sets.difference(updatedAttrNameAttrMap.keySet(), existingAttrNameAttrMap.keySet());
		if (!addedAttrNames.isEmpty())
		{
			// generate attribute identifiers
			Map<String, String> attrIdentifiers = new HashMap<>();
			addedAttrNames.stream().forEach(attrName -> attrIdentifiers.put(attrName, idGenerator.generateId()));

			List<Entity> attrEntitiesToAdd = addedAttrNames.stream()
					.map(addedAttrName -> updatedAttrNameAttrMap.get(addedAttrName)).collect(toList());
			injectAttrEntityIdentifiersRec(attrEntitiesToAdd, attrIdentifiers);

			getAttributeRepository().add(attrEntitiesToAdd.stream());

			// FIXME find more elegant way
			attrEntitiesToAdd.forEach(attrEntity -> {
				((DefaultAttributeMetaData) updatedEntityMeta
						.getAttribute(attrEntity.getString(AttributeMetaDataMetaData.NAME)))
								.setIdentifier(attrEntity.getString(AttributeMetaDataMetaData.IDENTIFIER));
			});
		}

		// update existing attributes
		Set<String> existingAttrNames = Sets.intersection(existingAttrNameAttrMap.keySet(),
				updatedAttrNameAttrMap.keySet());
		if (!existingAttrNames.isEmpty())
		{
			Stream<Entity> attrsToUpdate = existingAttrNames.stream().map(attrToUpdateName -> {
				// copy identifiers of existing attributes to updated attributes
				Entity attrEntity = existingAttrNameAttrMap.get(attrToUpdateName);
				Entity updatedAttrEntity = updatedAttrNameAttrMap.get(attrToUpdateName);
				updatedAttrEntity.set(AttributeMetaDataMetaData.IDENTIFIER,
						attrEntity.getString(AttributeMetaDataMetaData.IDENTIFIER));
				return updatedAttrEntity;
			}).filter(attrEntity -> {
				// determine which attributes are updated
				String attrName = attrEntity.getString(AttributeMetaDataMetaData.NAME);
				return !MetaUtils.equals(attrEntity, updatedEntityMeta.getAttribute(attrName));
			});
			getAttributeRepository().update(attrsToUpdate);
		}

		// update entity
		if (!MetaUtils.equals(existingEntityMetaEntity, updatedEntityMeta, this))
		{
			getEntityRepository().update(updatedEntityMetaEntity);
		}

		// delete attributes
		Set<String> deletedAttrNames = Sets.difference(existingAttrNameAttrMap.keySet(),
				updatedAttrNameAttrMap.keySet());
		if (!deletedAttrNames.isEmpty())
		{
			Stream<Entity> attrEntitiesToDelete = deletedAttrNames.stream()
					.map(deletedAttrName -> existingAttrNameAttrMap.get(deletedAttrName));
			getAttributeRepository().delete(attrEntitiesToDelete);
		}

		return Collections.emptyList();
	}

	@Override
	public void upsertEntityMeta(EntityMetaData entityMeta)
	{
		if (doAddEntityMeta(entityMeta))
		{
			addEntityMeta(entityMeta);
		}
		else
		{
			updateEntityMeta(entityMeta);
		}
	}

	@Transactional
	@Override
	public void addAttribute(String fullyQualifiedEntityName, AttributeMetaData attr)
	{
		EntityMetaData entityMeta = getEntityMetaData(fullyQualifiedEntityName);
		DefaultEntityMetaData updatedEntityMeta = new DefaultEntityMetaData(entityMeta);
		updatedEntityMeta.addAttributeMetaData(attr);
		updateEntityMeta(updatedEntityMeta);
	}

	@Override
	public EntityMetaData getEntityMetaData(String fullyQualifiedEntityName)
	{
		EntityMetaData systemEntity = systemEntityMetaDataRegistry.getSystemEntity(fullyQualifiedEntityName);
		if (systemEntity != null)
		{
			return systemEntity;
		}
		else
		{
			Entity entityEntity = getEntityRepository().findOne(fullyQualifiedEntityName);
			return entityEntity != null
					? MetaUtils.toEntityMeta(entityEntity, getEntityRepository(), languageService.getLanguageCodes())
					: null;
		}
	}

	@Override
	public void addPackage(Package package_)
	{
		LOG.info(format("Creating package [%s]", package_.getName()));
		getPackageRepository().add(MetaUtils.toEntity(package_));
	}

	public void updatePackage(Package package_)
	{
		LOG.info(format("Updating package [%s]", package_.getName()));
		getPackageRepository().update(MetaUtils.toEntity(package_));
	}

	@Override
	public void upsertPackage(Package package_)
	{
		if (doAddPackage(package_))
		{
			addPackage(package_);
		}
		else if (doUpdatePackage(package_))
		{
			updatePackage(package_);
		}
	}

	@Override
	public Package getDefaultPackage()
	{
		return defaultPackage;
	}

	@Override
	public Package getPackage(String packageName)
	{
		Entity packageEntity = getPackageRepository().findOne(packageName);
		return packageEntity != null ? MetaUtils.toPackage(packageEntity) : null;
	}

	@Override
	public List<Package> getPackages()
	{
		return getPackageRepository().stream().map(MetaUtils::toPackage).collect(toList());
	}

	@Override
	public List<Package> getRootPackages()
	{
		return getPackageRepository().query().eq(PackageMetaData.PARENT, null).findAll().map(MetaUtils::toPackage)
				.collect(toList());
	}

	/**
	 * Empties all metadata tables for the sake of testability.
	 */
	@Transactional
	public void recreateMetaDataRepositories()
	{
		delete(Lists.newArrayList(getEntityMetaDatas()));
		getAttributeRepository().deleteAll();
		getEntityRepository().deleteAll();
		getPackageRepository().deleteAll();
	}

	@Override
	public Collection<EntityMetaData> getEntityMetaDatas()
	{
		return getEntityRepository().stream().map(entityEntity -> MetaUtils.toEntityMeta(entityEntity,
				getEntityRepository(), languageService.getLanguageCodes())).collect(toList());
	}

	// TODO make private
	@Override
	public void refreshCaches()
	{

	}

	@Override
	public Iterator<RepositoryCollection> iterator()
	{
		return repositoryCollectionRegistry.stream().iterator();
	}

	@Override
	public boolean hasBackend(String backendName)
	{
		return repositoryCollectionRegistry.hasRepositoryCollection(backendName);
	}

	@Override
	public boolean isMetaRepository(String entityName)
	{
		switch (entityName)
		{
			case AttributeMetaDataMetaData.ENTITY_NAME:
			case EntityMetaDataMetaData.ENTITY_NAME:
			case PackageMetaData.ENTITY_NAME:
			case LanguageMetaData.ENTITY_NAME:
			case TagMetaData.ENTITY_NAME:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Returns child attributes of the given attribute in post-order
	 * 
	 * @param attr
	 * @return
	 */
	private Stream<AttributeMetaData> getAttributesPostOrder(AttributeMetaData attr)
	{
		return StreamSupport.stream(new TreeTraverser<AttributeMetaData>()
		{
			@Override
			public Iterable<AttributeMetaData> children(AttributeMetaData attr)
			{
				return attr.getDataType() instanceof CompoundField ? attr.getAttributeParts() : emptyList();
			}
		}.postOrderTraversal(attr).spliterator(), false);
	}

	/**
	 * Generate and inject attribute identifier
	 * 
	 * @param attr
	 * @return
	 */
	private AttributeMetaData generateAttrIdentifier(AttributeMetaData attr)
	{
		// generate attribute identifiers
		((DefaultAttributeMetaData) attr).setIdentifier(idGenerator.generateId());
		return attr;
	}

	private Map<String, Entity> createAttrIdentifierMap(Entity entityMetaEntity)
	{
		Map<String, Entity> nameAttrEntityMap = new HashMap<>();
		Iterable<Entity> attrEntities = entityMetaEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES);
		createAttrIdentifierMapRec(attrEntities, nameAttrEntityMap);
		return nameAttrEntityMap;
	}

	private void createAttrIdentifierMapRec(Iterable<Entity> attrEntities, Map<String, Entity> nameAttrEntityMap)
	{
		attrEntities.forEach(attrEntity -> {
			String attrName = attrEntity.getString(AttributeMetaDataMetaData.NAME);
			nameAttrEntityMap.put(attrName, attrEntity);
			if (attrEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)
					.equals(FieldTypeEnum.COMPOUND.toString().toLowerCase()))
			{
				Iterable<Entity> attrPartEntities = attrEntity.getEntities(AttributeMetaDataMetaData.PARTS);
				createAttrIdentifierMapRec(attrPartEntities, nameAttrEntityMap);
			}
		});
	}

	private void injectAttrEntityIdentifiers(Entity entityMetaEntity, Map<String, String> attrIdentifiers)
	{
		Iterable<Entity> attrEntities = entityMetaEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES);
		injectAttrEntityIdentifiersRec(attrEntities, attrIdentifiers);

		Entity idAttrEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE);
		if (idAttrEntity != null)
		{
			injectAttrEntityIdentifiersRec(singleton(idAttrEntity), attrIdentifiers);
		}

		Entity labelAttrEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE);
		if (labelAttrEntity != null)
		{
			injectAttrEntityIdentifiersRec(singleton(labelAttrEntity), attrIdentifiers);
		}

		Iterable<Entity> lookupAttrEntities = entityMetaEntity.getEntities(EntityMetaDataMetaData.LOOKUP_ATTRIBUTES);
		injectAttrEntityIdentifiersRec(lookupAttrEntities, attrIdentifiers);
	}

	private void injectAttrEntityIdentifiersRec(Iterable<Entity> attrEntities, Map<String, String> attrIdentifiers)
	{
		attrEntities.forEach(attrEntity -> {
			String attrIdentifier = attrEntity.getString(AttributeMetaDataMetaData.IDENTIFIER);
			if (attrIdentifier == null)
			{
				String attrName = attrEntity.getString(AttributeMetaDataMetaData.NAME);
				attrIdentifier = attrIdentifiers.get(attrName);
				if (attrIdentifier != null)
				{
					attrEntity.set(AttributeMetaDataMetaData.IDENTIFIER, attrIdentifier);
				}
			}
			if (attrEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)
					.equals(FieldTypeEnum.COMPOUND.toString().toLowerCase()))
			{
				Iterable<Entity> attrParts = attrEntity.getEntities(AttributeMetaDataMetaData.PARTS);
				injectAttrEntityIdentifiersRec(attrParts, attrIdentifiers);
			}
		});
	}

	private Repository getPackageRepository()
	{
		return getDefaultBackend().getRepository(packageMetaData);
	}

	private Repository getEntityRepository()
	{
		return getDefaultBackend().getRepository(entityMetaDataMetaData);
	}

	private Repository getAttributeRepository()
	{
		return getDefaultBackend().getRepository(attributeMetaDataMetaData);
	}

	private boolean doAddPackage(Package package_)
	{
		return getPackageRepository().findOne(package_.getName(), new Fetch().field(PackageMetaData.FULL_NAME)) == null;
	}

	private boolean doUpdatePackage(Package package_)
	{
		Entity existingPackageEntity = getPackageRepository().findOne(package_.getName());
		return existingPackageEntity != null && !MetaUtils.equals(existingPackageEntity, package_);
	}

	private boolean doAddEntityMeta(EntityMetaData entityMeta)
	{
		return !getDefaultBackend().hasRepository(entityMeta.getName());
	}

	@Override
	public void createRepository(EntityMetaData entityMeta)
	{
		String backendName = entityMeta.getBackend();
		getBackend(backendName).createRepository(entityMeta);
	}

	@Override
	public Repository getRepository(EntityMetaData entityMeta)
	{
		String backendName = entityMeta.getBackend();
		RepositoryCollection backend = getBackend(backendName);
		return backend.getRepository(entityMeta);
	}

	@Override
	public Repository getRepository(String entityName)
	{
		EntityMetaData entityMeta = getEntityMetaData(entityName);
		String backendName = entityMeta.getBackend();
		RepositoryCollection backend = getBackend(backendName);
		return backend.getRepository(entityMeta);
	}
}