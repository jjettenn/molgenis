package org.molgenis.data.elasticsearch;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.DataService;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Fetch;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.UnknownAttributeException;
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.meta.EntityMetaDataMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component("ElasticsearchRepositoryCollection")
public class ElasticsearchRepositoryCollection implements RepositoryCollection
{
	public static final String NAME = "ElasticSearch";
	private final SearchService searchService;
	private final DataService dataService;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;

	@Autowired
	public ElasticsearchRepositoryCollection(SearchService searchService, DataService dataService,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry)
	{
		this.searchService = requireNonNull(searchService);
		this.dataService = requireNonNull(dataService);
		this.systemEntityMetaDataRegistry = requireNonNull(systemEntityMetaDataRegistry);
	}

	@Override
	public Repository createRepository(EntityMetaData entityMeta)
	{
		ElasticsearchRepository repo = new ElasticsearchRepository(entityMeta, searchService);
		if (!searchService.hasMapping(entityMeta)) repo.create();
		// repositories.put(entityMeta.getName(), repo);

		return repo;
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		return new Iterable<String>()
		{
			@Override
			public Iterator<String> iterator()
			{
				return dataService.getRepository(EntityMetaDataMetaData.ENTITY_NAME).query()
						.eq(EntityMetaDataMetaData.BACKEND, NAME)
						.fetch(new Fetch().field(EntityMetaDataMetaData.FULL_NAME)).findAll()
						.map(entity -> entity.getString(EntityMetaDataMetaData.FULL_NAME)).iterator();
			}
		};
	}

	@Override
	public Repository getRepository(String name)
	{
		return new ElasticsearchRepository(dataService.getMeta().getEntityMetaData(name), searchService);
	}

	@Override
	public Repository getRepository(EntityMetaData entityMeta)
	{
		return new ElasticsearchRepository(entityMeta, searchService);
	}

	@Override
	public Iterator<Repository> iterator()
	{
		// FIXME code duplication
		return dataService.getRepository(EntityMetaDataMetaData.ENTITY_NAME).query()
				.eq(EntityMetaDataMetaData.BACKEND, NAME).fetch(new Fetch().field(EntityMetaDataMetaData.FULL_NAME))
				.findAll().map(entity -> entity.getString(EntityMetaDataMetaData.FULL_NAME)).map(this::getRepository)
				.iterator();
	}

	@Override
	public void deleteEntityMeta(String entityName)
	{
		throw new UnsupportedOperationException("TODO implement"); // FIXME
	}

	@Override
	public void addAttribute(String entityName, AttributeMetaData attribute)
	{
		DefaultEntityMetaData entityMetaData;
		try
		{
			entityMetaData = (DefaultEntityMetaData) dataService.getEntityMetaData(entityName);
		}
		catch (ClassCastException ex)
		{
			throw new RuntimeException("Cannot cast EntityMetaData to DefaultEntityMetadata " + ex);
		}
		if (entityMetaData == null) throw new UnknownEntityException(String.format("Unknown entity '%s'", entityName));

		entityMetaData.addAttributeMetaData(attribute);
		searchService.createMappings(entityMetaData);
	}

	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		EntityMetaData entityMetaData = dataService.getMeta().getEntityMetaData(entityName);
		if (entityMetaData == null) throw new UnknownEntityException(String.format("Unknown entity '%s'", entityName));

		DefaultEntityMetaData defaultEntityMetaData = new DefaultEntityMetaData(
				dataService.getMeta().getEntityMetaData(entityName));
		AttributeMetaData attr = entityMetaData.getAttribute(attributeName);
		if (attr == null) throw new UnknownAttributeException(
				String.format("Unknown attribute '%s' of entity '%s'", attributeName, entityName));

		defaultEntityMetaData.removeAttributeMetaData(attr);
		searchService.createMappings(entityMetaData);
	}

	@Override
	public void addAttributeSync(String entityName, AttributeMetaData attribute)
	{
		addAttribute(entityName, attribute);
	}

	@Override
	public boolean hasRepository(String name)
	{
		EntityMetaData systemEntity = systemEntityMetaDataRegistry.getSystemEntity(name);
		if (systemEntity != null)
		{
			return systemEntity.getBackend().equals(NAME);
		}
		else
		{
			return dataService.getRepository(EntityMetaDataMetaData.ENTITY_NAME).query()
					.eq(EntityMetaDataMetaData.FULL_NAME, name).and().eq(EntityMetaDataMetaData.BACKEND, NAME)
					.count() > 0;
		}
	}

	@Override
	public void initMetaDataRepositories(ApplicationContext ctx)
	{
		throw new UnsupportedOperationException();
	}
}
