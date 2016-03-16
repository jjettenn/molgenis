package org.molgenis.data.support;

import static java.lang.String.format;
import static org.molgenis.security.core.utils.SecurityUtils.getCurrentUsername;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.molgenis.data.AggregateQuery;
import org.molgenis.data.AggregateResult;
import org.molgenis.data.DataService;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityListener;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Fetch;
import org.molgenis.data.MolgenisDataAccessException;
import org.molgenis.data.Query;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCapability;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.security.core.utils.SecurityUtils;
import org.molgenis.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of the DataService interface
 */

public class DataServiceImpl implements DataService
{
	private static final Logger LOG = LoggerFactory.getLogger(DataServiceImpl.class);

	private MetaDataService metaDataService;

	/**
	 * For testing purposes
	 */
	public synchronized void resetRepositories()
	{
		// repositories.clear();
		// repositoryNames.clear();
	}

	@Override
	public void setMeta(MetaDataService metaDataService)
	{
		this.metaDataService = metaDataService;
	}

	// FIXME remove
	public synchronized void addRepository(Repository newRepository)
	{
		// Entity repoEntity = MetaUtils.toEntity(newRepository,
		// metaDataService.getBackend(newRepository.getEntityMetaData()));
		// getRepositoryRepository().add(repoEntity);
		// String repositoryName = newRepository.getName();
		// if (repositories.containsKey(repositoryName.toLowerCase()))
		// {
		// throw new MolgenisDataException("Entity [" + repositoryName + "] already registered.");
		// }
		// if (LOG.isDebugEnabled()) LOG.debug("Adding repository [" + repositoryName + "]");
		// repositoryNames.add(repositoryName);
		//
		// Repository decoratedRepo = repositoryDecoratorFactory.createDecoratedRepository(newRepository);
		// repositories.put(repositoryName.toLowerCase(), decoratedRepo);
	}

	// FIXME remove
	public synchronized void removeRepository(String repositoryName)
	{
		// if (null == repositoryName)
		// {
		// throw new MolgenisDataException("repositoryName may not be null");
		// }
		// if (LOG.isDebugEnabled()) LOG.debug("Removing repository [" + repositoryName + "]");
		// getRepositoryRepository().deleteById(repositoryName);
	}

	@Override
	public EntityMetaData getEntityMetaData(String entityName)
	{
		if (SecurityUtils.currentUserHasRole("ROLE_SU", "ROLE_SYSTEM", "ROLE_ENTITY_COUNT_" + entityName.toUpperCase()))
		{
			EntityMetaData entityMeta = metaDataService.getEntityMetaData(entityName);
			if (entityMeta == null)
			{
				throw new UnknownEntityException(format("Unknown entity [%s]", entityName));
			}
			return entityMeta;
		}
		else
		{
			throw new MolgenisDataAccessException(); // TODO msg
		}
	}

	@Override
	public synchronized Stream<String> getEntityNames()
	{
		return metaDataService.stream().flatMap(repoCollection -> repoCollection.stream()).map(Repository::getName)
				.filter(repoName -> SecurityUtils.currentUserHasRole("ROLE_SU", "ROLE_SYSTEM",
						"ROLE_ENTITY_COUNT_" + repoName.toUpperCase()));
	}

	@Override
	public boolean hasRepository(String entityName)
	{
		// FIXME security
		return metaDataService.getEntityMetaData(entityName) != null;
	}

	@Override
	public long count(String entityName, Query q)
	{
		return getRepository(entityName).count(q);
	}

	@Override
	public Stream<Entity> findAll(String entityName)
	{
		return findAll(entityName, new QueryImpl());
	}

	@Override
	public Stream<Entity> findAll(String entityName, Query q)
	{
		return getRepository(entityName).findAll(q);
	}

	@Override
	public Entity findOne(String entityName, Object id)
	{
		return getRepository(entityName).findOne(id);
	}

	@Override
	public Entity findOne(String entityName, Query q)
	{
		return getRepository(entityName).findOne(q);
	}

	@Override
	@Transactional
	public void add(String entityName, Entity entity)
	{
		getRepository(entityName).add(entity);
	}

	@Override
	@Transactional
	public void add(String entityName, Stream<? extends Entity> entities)
	{
		getRepository(entityName).add(entities);
	}

	@Override
	@Transactional
	public void update(String entityName, Entity entity)
	{
		getRepository(entityName).update(entity);
	}

	@Override
	@Transactional
	public void update(String entityName, Stream<? extends Entity> entities)
	{
		getRepository(entityName).update(entities);
	}

	@Override
	@Transactional
	public void delete(String entityName, Entity entity)
	{
		getRepository(entityName).delete(entity);
	}

	@Override
	@Transactional
	public void delete(String entityName, Stream<? extends Entity> entities)
	{
		getRepository(entityName).delete(entities);
	}

	@Override
	@Transactional
	public void delete(String entityName, Object id)
	{
		getRepository(entityName).deleteById(id);
	}

	@Override
	@Transactional
	public void deleteAll(String entityName)
	{
		getRepository(entityName).deleteAll();
		LOG.info("All entities of repository [{}] deleted by user [{}]", entityName, getCurrentUsername());
	}

	@Override
	public Repository getRepository(String entityName)
	{
		return metaDataService.getRepository(entityName);
		// Repository repo = metaDataService.stream().filter(repoCollection -> {
		// return repoCollection.hasRepository(entityName);
		// }).map(repoCollection -> repoCollection.getRepository(entityName)).findFirst().orElse(null);
		// if (repo == null)
		// {
		// throw new UnknownEntityException(format("unknown entity [%s]", entityName));
		// }
		// return repo;
		// Entity repoEntity = getRepositoryRepository().findOne(entityName);
		// if (repoEntity == null)
		// {
		// throw new UnknownEntityException("Unknown entity [" + entityName + "]");
		// }
		// Repository undecoratedRepo = MetaUtils.toRepository(repoEntity);
		// return repositoryDecoratorFactory.createDecoratedRepository(undecoratedRepo);
	}

	@Override
	public Query query(String entityName)
	{
		return new QueryImpl(getRepository(entityName));
	}

	@Override
	public <E extends Entity> Stream<E> findAll(String entityName, Query q, Class<E> clazz)
	{
		Stream<Entity> entities = getRepository(entityName).findAll(q);
		return entities.map(entity -> {
			return EntityUtils.convert(entity, clazz, this);
		});
	}

	@Override
	public <E extends Entity> E findOne(String entityName, Object id, Class<E> clazz)
	{
		Entity entity = getRepository(entityName).findOne(id);
		if (entity == null) return null;
		return EntityUtils.convert(entity, clazz, this);
	}

	@Override
	public <E extends Entity> E findOne(String entityName, Query q, Class<E> clazz)
	{
		Entity entity = getRepository(entityName).findOne(q);
		if (entity == null) return null;
		return EntityUtils.convert(entity, clazz, this);
	}

	@Override
	public <E extends Entity> Stream<E> findAll(String entityName, Class<E> clazz)
	{
		return findAll(entityName, new QueryImpl(), clazz);
	}

	@Override
	public AggregateResult aggregate(String entityName, AggregateQuery aggregateQuery)
	{
		return getRepository(entityName).aggregate(aggregateQuery);
	}

	@Override
	public MetaDataService getMeta()
	{
		return metaDataService;
	}

	@Override
	public synchronized Iterator<Repository> iterator()
	{
		return metaDataService.stream().flatMap(RepositoryCollection::stream).iterator();
		// return getRepositoryRepository().stream().map(repoEntity -> {
		// Repository undecoratedRepo = MetaUtils.toRepository(repoEntity);
		// return repositoryDecoratorFactory.createDecoratedRepository(undecoratedRepo);
		// }).iterator();
	}

	// private Repository getRepositoryRepository()
	// {
	// return metaDataService.getDefaultBackend().getRepository(RepositoryMetaData.ENTITY_NAME);
	// }

	@Override
	public Stream<Entity> stream(String entityName, Fetch fetch)
	{
		return getRepository(entityName).stream(fetch);
	}

	@Override
	public <E extends Entity> Stream<E> stream(String entityName, Fetch fetch, Class<E> clazz)
	{
		Stream<Entity> entities = getRepository(entityName).stream(fetch);
		return entities.map(entity -> {
			return EntityUtils.convert(entity, clazz, this);
		});
	}

	@Override
	public Set<RepositoryCapability> getCapabilities(String repositoryName)
	{
		return getRepository(repositoryName).getCapabilities();
	}

	@Override
	public Entity findOne(String entityName, Object id, Fetch fetch)
	{
		return getRepository(entityName).findOne(id, fetch);
	}

	@Override
	public <E extends Entity> E findOne(String entityName, Object id, Fetch fetch, Class<E> clazz)
	{
		Entity entity = getRepository(entityName).findOne(id, fetch);
		if (entity == null) return null;
		return EntityUtils.convert(entity, clazz, this);
	}

	@Override
	public void addEntityListener(String entityName, EntityListener entityListener)
	{
		getRepository(entityName).addEntityListener(entityListener);
	}

	@Override
	public void removeEntityListener(String entityName, EntityListener entityListener)
	{
		getRepository(entityName).removeEntityListener(entityListener);
	}

	@Override
	public Stream<Entity> findAll(String entityName, Stream<Object> ids)
	{
		return getRepository(entityName).findAll(ids);
	}

	@Override
	public <E extends Entity> Stream<E> findAll(String entityName, Stream<Object> ids, Class<E> clazz)
	{
		Stream<Entity> entities = getRepository(entityName).findAll(ids);
		return entities.map(entity -> {
			return EntityUtils.convert(entity, clazz, this);
		});
	}

	@Override
	public Stream<Entity> findAll(String entityName, Stream<Object> ids, Fetch fetch)
	{
		return getRepository(entityName).findAll(ids, fetch);
	}

	@Override
	public <E extends Entity> Stream<E> findAll(String entityName, Stream<Object> ids, Fetch fetch, Class<E> clazz)
	{
		Stream<Entity> entities = getRepository(entityName).findAll(ids, fetch);
		return entities.map(entity -> {
			return EntityUtils.convert(entity, clazz, this);
		});
	}

	@Override
	public Repository copyRepository(Repository repository, String newRepositoryId, String newRepositoryLabel)
	{
		return copyRepository(repository, newRepositoryId, newRepositoryLabel, new QueryImpl());
	}

	@Override
	public Repository copyRepository(Repository repository, String newRepositoryId, String newRepositoryLabel,
			Query query)
	{
		LOG.info("Creating a copy of " + repository.getName() + " repository, with ID: " + newRepositoryId
				+ ", and label: " + newRepositoryLabel);
		DefaultEntityMetaData emd = new DefaultEntityMetaData(newRepositoryId, repository.getEntityMetaData());
		emd.setLabel(newRepositoryLabel);
		Repository repositoryCopy = metaDataService.addEntityMeta(emd);
		repositoryCopy.add(repository.findAll(query));
		return repositoryCopy;
	}
}
