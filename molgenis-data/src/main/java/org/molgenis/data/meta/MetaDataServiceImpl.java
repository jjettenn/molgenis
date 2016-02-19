package org.molgenis.data.meta;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.molgenis.util.SecurityDecoratorUtils.validatePermission;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.molgenis.MolgenisFieldTypes;
import org.molgenis.MolgenisFieldTypes.FieldTypeEnum;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.IdGenerator;
import org.molgenis.data.ManageableRepositoryCollection;
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.Package;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.RepositoryDecoratorFactory;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.i18n.I18nStringMetaData;
import org.molgenis.data.i18n.LanguageMetaData;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.meta.system.ImportRunMetaData;
import org.molgenis.data.support.DataServiceImpl;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.molgenis.data.support.NonDecoratingRepositoryDecoratorFactory;
import org.molgenis.data.support.UuidGenerator;
import org.molgenis.security.core.Permission;
import org.molgenis.security.core.runas.RunAsSystem;
import org.molgenis.security.core.runas.RunAsSystemProxy;
import org.molgenis.security.core.utils.SecurityUtils;
import org.molgenis.util.DependencyResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
	private PackageRepository packageRepository;
	private EntityMetaDataRepository entityMetaDataRepository;
	private AttributeMetaDataRepository attributeMetaDataRepository;
	private ManageableRepositoryCollection defaultBackend;
	private final Map<String, RepositoryCollection> backends = Maps.newHashMap();
	private final DataServiceImpl dataService;
	private TransactionTemplate transactionTemplate;
	private LanguageService languageService;
	private IdGenerator idGenerator;

	public MetaDataServiceImpl(DataServiceImpl dataService)
	{
		this.dataService = dataService;
	}

	@Autowired
	public void setIdGenerator(IdGenerator idGenerator)
	{
		this.idGenerator = idGenerator;
	}

	@Autowired
	public void setLanguageService(LanguageService languageService)
	{
		this.languageService = languageService;
	}

	@Autowired
	public void setPlatformTransactionManager(PlatformTransactionManager transactionManager)
	{
		this.transactionTemplate = new TransactionTemplate(transactionManager);
	}

	/**
	 * Sets the Backend, in wich the meta data and the user data is saved
	 * 
	 * Setter for the ManageableCrudRepositoryCollection, to be called after it's created. This resolves the circular
	 * dependency {@link MysqlRepositoryCollection} => decorated {@link MetaDataService} => {@link RepositoryCreator}
	 * 
	 * @param ManageableRepositoryCollection
	 */
	@Override
	public MetaDataService setDefaultBackend(ManageableRepositoryCollection backend)
	{
		this.defaultBackend = backend;
		backends.put(backend.getName(), backend);

		I18nStringMetaData.INSTANCE.setBackend(backend.getName());
		LanguageMetaData.INSTANCE.setBackend(backend.getName());
		PackageMetaData.INSTANCE.setBackend(backend.getName());
		TagMetaData.INSTANCE.setBackend(backend.getName());
		EntityMetaDataMetaData.INSTANCE.setBackend(backend.getName());
		AttributeMetaDataMetaData.INSTANCE.setBackend(backend.getName());

		ImportRunMetaData.INSTANCE.setBackend(backend.getName());

		bootstrapMetaRepos();
		return this;
	}

	private void bootstrapMetaRepos()
	{
		MetaDataRepositoryDecoratorFactory metaRepoDecoratorFactory = new MetaDataRepositoryDecoratorFactory(
				dataService, this, new SystemEntityMetaDataRegistry(dataService), languageService);

		Repository languageRepo = defaultBackend.addEntityMeta(LanguageMetaData.INSTANCE);
		dataService.addRepository(metaRepoDecoratorFactory.createDecoratedRepository(languageRepo));

		Repository i18StringsRepo = defaultBackend.addEntityMeta(I18nStringMetaData.INSTANCE);
		dataService.addRepository(metaRepoDecoratorFactory.createDecoratedRepository(i18StringsRepo));

		Supplier<Stream<String>> languageCodes = () -> languageService.getLanguageCodes().stream();

		// Add language attributes to the AttributeMetaDataMetaData
		languageCodes.get().map(code -> AttributeMetaDataMetaData.LABEL + '-' + code)
				.forEach(AttributeMetaDataMetaData.INSTANCE::addAttribute);

		// Add description attributes to the AttributeMetaDataMetaData
		languageCodes.get().map(code -> AttributeMetaDataMetaData.DESCRIPTION + '-' + code)
				.forEach(attrName -> AttributeMetaDataMetaData.INSTANCE.addAttribute(attrName)
						.setDataType(MolgenisFieldTypes.TEXT));

		// Add description attributes to the EntityMetaDataMetaData
		languageCodes.get().map(code -> EntityMetaDataMetaData.DESCRIPTION + '-' + code)
				.forEach(attrName -> EntityMetaDataMetaData.INSTANCE.addAttribute(attrName)
						.setDataType(MolgenisFieldTypes.TEXT));

		// Add language attributes to the EntityMetaDataMetaData
		languageCodes.get().map(code -> EntityMetaDataMetaData.LABEL + '-' + code)
				.forEach(EntityMetaDataMetaData.INSTANCE::addAttribute);

		// Add language attributes to I18nStringMetaData
		languageCodes.get().forEach(I18nStringMetaData.INSTANCE::addLanguage);

		Repository tagRepo = defaultBackend.addEntityMeta(TagMetaData.INSTANCE);
		dataService.addRepository(metaRepoDecoratorFactory.createDecoratedRepository(tagRepo));

		Repository packages = defaultBackend.addEntityMeta(PackageRepository.META_DATA);
		dataService.addRepository(metaRepoDecoratorFactory.createDecoratedRepository(packages));
		packageRepository = new PackageRepository(packages);

		attributeMetaDataRepository = new AttributeMetaDataRepository(defaultBackend, languageService, idGenerator);
		entityMetaDataRepository = new EntityMetaDataRepository(defaultBackend, packageRepository,
				attributeMetaDataRepository, languageService);
		attributeMetaDataRepository.setEntityMetaDataRepository(entityMetaDataRepository);

		dataService.addRepository(
				metaRepoDecoratorFactory.createDecoratedRepository(attributeMetaDataRepository.getRepository()));
		dataService.addRepository(
				metaRepoDecoratorFactory.createDecoratedRepository(entityMetaDataRepository.getRepository()));
		entityMetaDataRepository.fillEntityMetaDataCache();
	}

	@Override
	public ManageableRepositoryCollection getDefaultBackend()
	{
		return defaultBackend;
	}

	@Override
	public RepositoryCollection getBackend(String name)
	{
		return backends.get(name);
	}

	/**
	 * Removes entity meta data if it exists.
	 */
	@Override
	public void deleteEntityMeta(String entityName)
	{
		validatePermission(entityName, Permission.WRITEMETA);

		transactionTemplate.execute((TransactionStatus status) -> {
			EntityMetaData emd = getEntityMetaData(entityName);
			if ((emd != null) && !emd.isAbstract())
			{
				getManageableRepositoryCollection(emd).deleteEntityMeta(entityName);
			}
			entityMetaDataRepository.delete(entityName);
			if (dataService.hasRepository(entityName)) dataService.removeRepository(entityName);
			deleteEntityPermissions(entityName);

			return null;
		});

		refreshCaches();
	}

	private void deleteEntityPermissions(String entityName)
	{
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

	@Transactional
	@Override
	public void delete(List<EntityMetaData> entities)
	{
		entities.forEach(emd -> validatePermission(emd.getName(), Permission.WRITEMETA));

		reverse(DependencyResolver.resolve(Sets.newHashSet(entities))).stream().map(EntityMetaData::getName)
				.forEach(this::deleteEntityMeta);
	}

	/**
	 * Removes an attribute from an entity.
	 */
	@Transactional
	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		validatePermission(entityName, Permission.WRITEMETA);

		// Update AttributeMetaDataRepository
		entityMetaDataRepository.removeAttribute(entityName, attributeName);
		EntityMetaData emd = getEntityMetaData(entityName);
		if (emd != null) getManageableRepositoryCollection(emd).deleteAttribute(entityName, attributeName);
	}

	private ManageableRepositoryCollection getManageableRepositoryCollection(EntityMetaData emd)
	{
		RepositoryCollection backend = getBackend(emd);
		if (!(backend instanceof ManageableRepositoryCollection))
			throw new RuntimeException("Backend  is not a ManageableCrudRepositoryCollection");

		return (ManageableRepositoryCollection) backend;
	}

	@Override
	public RepositoryCollection getBackend(EntityMetaData emd)
	{
		String backendName = emd.getBackend() == null ? getDefaultBackend().getName() : emd.getBackend();
		RepositoryCollection backend = backends.get(backendName);
		if (backend == null) throw new RuntimeException("Unknown backend [" + backendName + "]");

		return backend;
	}

	@Transactional
	@Override
	public synchronized Repository add(EntityMetaData emd, RepositoryDecoratorFactory decoratorFactory)
	{
		MetaValidationUtils.validateEntityMetaData(emd);
		RepositoryCollection backend = getBackend(emd);

		if (getEntityMetaData(emd.getName()) != null)
		{
			if (emd.isAbstract()) return null;

			if (!dataService.hasRepository(emd.getName()))
			{
				Repository repo = backend.getRepository(emd.getName());
				if (repo == null) throw new UnknownEntityException(
						String.format("Unknown entity '%s' for backend '%s'", emd.getName(), backend.getName()));
				Repository decoratedRepo = decoratorFactory.createDecoratedRepository(repo);
				dataService.addRepository(decoratedRepo);
			}

			// Return decorated repo
			return dataService.getRepository(emd.getName());
		}

		if (dataService.hasRepository(emd.getName()))
		{
			throw new MolgenisDataException("Entity with name [" + emd.getName() + "] already exists.");
		}

		if (emd.getPackage() != null)
		{
			packageRepository.add(emd.getPackage());
		}

		addToEntityMetaDataRepository(emd);
		if (emd.isAbstract()) return null;

		Repository repo = backend.addEntityMeta(getEntityMetaData(emd.getName()));
		Repository decoratedRepo = decoratorFactory.createDecoratedRepository(repo);

		dataService.addRepository(decoratedRepo);

		// Return decorated repo
		return dataService.getRepository(emd.getName());
	}

	@Transactional
	@Override
	public Repository addEntityMeta(EntityMetaData emd)
	{
		return add(emd, new NonDecoratingRepositoryDecoratorFactory());
	}

	@Transactional
	@Override
	public void addAttribute(String fullyQualifiedEntityName, AttributeMetaData attr)
	{
		validatePermission(fullyQualifiedEntityName, Permission.WRITEMETA);
		MetaValidationUtils.validateName(attr.getName());

		EntityMetaData emd = entityMetaDataRepository.addAttribute(fullyQualifiedEntityName, attr);
		getManageableRepositoryCollection(emd).addAttribute(fullyQualifiedEntityName, attr);
	}

	@Override
	public void addAttributeSync(String fullyQualifiedEntityName, AttributeMetaData attr)
	{
		validatePermission(fullyQualifiedEntityName, Permission.WRITEMETA);
		MetaValidationUtils.validateName(attr.getName());

		EntityMetaData emd = entityMetaDataRepository.addAttribute(fullyQualifiedEntityName, attr);
		getManageableRepositoryCollection(emd).addAttributeSync(fullyQualifiedEntityName, attr);
	}

	@Override
	public DefaultEntityMetaData getEntityMetaData(String fullyQualifiedEntityName)
	{
		// at construction time, will be called when entityMetaDataRepository is still null
		if (attributeMetaDataRepository == null)
		{
			return null;
		}
		return entityMetaDataRepository.get(fullyQualifiedEntityName);
	}

	@Override
	public void addPackage(Package p)
	{
		MetaValidationUtils.validateName(p.getName());
		packageRepository.add(p);
	}

	@Override
	public Package getPackage(String string)
	{
		return packageRepository.getPackage(string);
	}

	@Override
	public List<Package> getPackages()
	{
		return packageRepository.getPackages();
	}

	@Override
	public List<Package> getRootPackages()
	{
		return packageRepository.getRootPackages();
	}

	/**
	 * Empties all metadata tables for the sake of testability.
	 */
	@Transactional
	public void recreateMetaDataRepositories()
	{
		delete(newArrayList(getEntityMetaDatas()));

		attributeMetaDataRepository.deleteAll();
		entityMetaDataRepository.deleteAll();
		packageRepository.deleteAll();
		packageRepository.updatePackageCache();
	}

	@Override
	public Collection<EntityMetaData> getEntityMetaDatas()
	{
		return entityMetaDataRepository.getMetaDatas();
	}

	// TODO make private
	@Override
	public void refreshCaches()
	{
		RunAsSystemProxy.runAsSystem(() -> {
			packageRepository.updatePackageCache();
			entityMetaDataRepository.fillEntityMetaDataCache();
			return null;
		});
	}

	@Transactional
	@Override
	public List<AttributeMetaData> updateEntityMeta(EntityMetaData updatedEntityMeta)
	{
		// TODO where to delete entities for which java class was deleted?

		Entity existingEntityMetaEntity = dataService.findOne(EntityMetaDataMetaData.ENTITY_NAME,
				updatedEntityMeta.getName());
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
			updatedEntityMetaEntity.set(EntityMetaDataMetaData.PACKAGE, MetaUtils.toEntity(PackageImpl.defaultPackage));
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
		injectAttrIdentifiers(updatedEntityMetaEntity, existingAttrNameAttrMap);
		Map<String, Entity> updatedAttrNameAttrMap = createAttrIdentifierMap(updatedEntityMetaEntity);

		// add new attributes
		Set<String> addedAttrNames = Sets.difference(updatedAttrNameAttrMap.keySet(), existingAttrNameAttrMap.keySet());
		if (!addedAttrNames.isEmpty())
		{
			// FIXME who will generate id --> autoid
			UuidGenerator uuidGenerator = new UuidGenerator();
			Stream<Entity> attrEntitiesToAdd = addedAttrNames.stream()
					.map(addedAttrName -> updatedAttrNameAttrMap.get(addedAttrName)).filter(attrEntity -> {
						String attrIdentifier = uuidGenerator.generateId();
						((DefaultAttributeMetaData) updatedEntityMeta
								.getAttribute(attrEntity.getString(AttributeMetaDataMetaData.NAME)))
										.setIdentifier(attrIdentifier); // FIXME find more elegant way
						attrEntity.set(AttributeMetaDataMetaData.IDENTIFIER, attrIdentifier);
						return true;
					});
			dataService.add(AttributeMetaDataMetaData.ENTITY_NAME, attrEntitiesToAdd);
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
			dataService.update(AttributeMetaDataMetaData.ENTITY_NAME, attrsToUpdate);
		}

		// update entity
		if (!MetaUtils.equals(existingEntityMetaEntity, updatedEntityMeta, this))
		{
			dataService.update(EntityMetaDataMetaData.ENTITY_NAME, updatedEntityMetaEntity);
		}

		// delete attributes
		Set<String> deletedAttrNames = Sets.difference(existingAttrNameAttrMap.keySet(),
				updatedAttrNameAttrMap.keySet());
		if (!deletedAttrNames.isEmpty())
		{
			Stream<Entity> attrEntitiesToDelete = deletedAttrNames.stream()
					.map(deletedAttrName -> existingAttrNameAttrMap.get(deletedAttrName));
			dataService.delete(AttributeMetaDataMetaData.ENTITY_NAME, attrEntitiesToDelete);
		}

		return Collections.emptyList();
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

	private void injectAttrIdentifiers(Entity entityMetaEntity, Map<String, Entity> existingAttrs)
	{
		Iterable<Entity> attrEntities = entityMetaEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES);
		injectAttrIdentifiersRec(attrEntities, existingAttrs);

		Entity idAttrEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE);
		if (idAttrEntity != null)
		{
			injectAttrIdentifiersRec(singleton(idAttrEntity), existingAttrs);
		}

		Entity labelAttrEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE);
		if (labelAttrEntity != null)
		{
			injectAttrIdentifiersRec(singleton(labelAttrEntity), existingAttrs);
		}

		Iterable<Entity> lookupAttrEntities = entityMetaEntity.getEntities(EntityMetaDataMetaData.LOOKUP_ATTRIBUTES);
		injectAttrIdentifiersRec(lookupAttrEntities, existingAttrs);
	}

	private void injectAttrIdentifiersRec(Iterable<Entity> attrEntities, Map<String, Entity> existingAttrs)
	{
		attrEntities.forEach(attrEntity -> {
			String attrIdentifier = attrEntity.getString(AttributeMetaDataMetaData.IDENTIFIER);
			if (attrIdentifier == null)
			{
				String attrName = attrEntity.getString(AttributeMetaDataMetaData.NAME);
				Entity existingAttrEntity = existingAttrs.get(attrName);
				if (existingAttrEntity != null)
				{
					String existingIdentifier = existingAttrEntity.getString(AttributeMetaDataMetaData.IDENTIFIER);
					attrEntity.set(AttributeMetaDataMetaData.IDENTIFIER, existingIdentifier);
				}
			}
			if (attrEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)
					.equals(FieldTypeEnum.COMPOUND.toString().toLowerCase()))
			{
				Iterable<Entity> attrParts = attrEntity.getEntities(AttributeMetaDataMetaData.PARTS);
				injectAttrIdentifiersRec(attrParts, existingAttrs);
			}
		});
	}

	@Override
	@Transactional
	public List<AttributeMetaData> updateSync(EntityMetaData sourceEntityMetaData)
	{

		return MetaUtils.updateEntityMeta(this, sourceEntityMetaData, true);
	}

	@Override
	public int getOrder()
	{
		return Ordered.HIGHEST_PRECEDENCE;
	}

	public void addBackend(RepositoryCollection backend)
	{
		backends.put(backend.getName(), backend);
	}

	@Override
	@RunAsSystem
	public synchronized void onApplicationEvent(ContextRefreshedEvent event)
	{
		// Discover all backends
		Map<String, RepositoryCollection> backendBeans = event.getApplicationContext()
				.getBeansOfType(RepositoryCollection.class);
		backendBeans.values().forEach(this::addBackend);

		Map<String, EntityMetaData> emds = event.getApplicationContext().getBeansOfType(EntityMetaData.class);

		// Create repositories from EntityMetaData in EntityMetaData repo
		for (EntityMetaData emd : entityMetaDataRepository.getMetaDatas())
		{
			if (!emd.isAbstract() && !dataService.hasRepository(emd.getName()))
			{
				RepositoryCollection col = backends.get(emd.getBackend());
				if (col == null) throw new MolgenisDataException("Unknown backend [" + emd.getBackend() + "]");
				Repository repo = col.addEntityMeta(emd);
				dataService.addRepository(repo);
			}
		}

		// Discover static EntityMetaData
		DependencyResolver.resolve(Sets.newHashSet(emds.values())).stream()
				.filter(emd -> !dataService.hasRepository(emd.getName())).forEach(this::addEntityMeta);

		// Update update manageable backends
		DependencyResolver.resolve(Sets.newHashSet(emds.values())).stream().filter(this::isManageableBackend)
				.forEach(this::updateEntityMeta);
	}

	private boolean isManageableBackend(EntityMetaData emd)
	{
		// Might work for more than just MySQL backend
		return emd.getBackend() == null || "MySql".equals(emd.getBackend());
	}

	@Override
	public Iterator<RepositoryCollection> iterator()
	{
		return backends.values().iterator();
	}

	public void updateEntityMetaBackend(String entityName, String backend)
	{
		validatePermission(entityName, Permission.WRITEMETA);

		DefaultEntityMetaData entityMeta = entityMetaDataRepository.get(entityName);
		if (entityMeta == null) throw new UnknownEntityException("Unknown entity '" + entityName + "'");
		entityMeta.setBackend(backend);
		entityMetaDataRepository.update(entityMeta);
	}

	public void addToEntityMetaDataRepository(EntityMetaData entityMetaData)
	{
		MetaValidationUtils.validateEntityMetaData(entityMetaData);
		entityMetaDataRepository.add(entityMetaData);
	}

	@Override
	public LinkedHashMap<String, Boolean> integrationTestMetaData(RepositoryCollection repositoryCollection)
	{
		LinkedHashMap<String, Boolean> entitiesImportable = new LinkedHashMap<String, Boolean>();
		StreamSupport.stream(repositoryCollection.getEntityNames().spliterator(), false)
				.forEach(entityName -> entitiesImportable.put(entityName, true));

		return entitiesImportable;
	}

	@Override
	public LinkedHashMap<String, Boolean> integrationTestMetaData(
			ImmutableMap<String, EntityMetaData> newEntitiesMetaDataMap, List<String> skipEntities,
			String defaultPackage)
	{
		LinkedHashMap<String, Boolean> entitiesImportable = new LinkedHashMap<String, Boolean>();

		StreamSupport.stream(newEntitiesMetaDataMap.keySet().spliterator(), false)
				.forEach(entityName -> entitiesImportable.put(entityName, skipEntities.contains(entityName)));

		return entitiesImportable;
	}

	@Override
	public boolean hasBackend(String backendName)
	{
		return backends.containsKey(backendName);
	}
}