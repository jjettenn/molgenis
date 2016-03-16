package org.molgenis.data.elasticsearch;

import static java.lang.String.format;

import java.util.Iterator;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.springframework.context.ApplicationContext;

/**
 * Adds indexing functionality to a RepositoryCollection
 */
public class IndexedRepositoryCollectionDecorator implements RepositoryCollection
{
	private final SearchService searchService;
	private RepositoryCollection delegate;

	public IndexedRepositoryCollectionDecorator(SearchService searchService, RepositoryCollection delegate)
	{
		this.searchService = searchService;
		this.delegate = delegate;
	}

	protected IndexedRepositoryCollectionDecorator(SearchService searchService)
	{
		this(searchService, null);
	}

	protected void setDelegate(RepositoryCollection delegate)
	{
		this.delegate = delegate;
	}

	protected RepositoryCollection getDelegate()
	{
		return delegate;
	}

	@Override
	public Iterator<Repository> iterator()
	{
		return new Iterator<Repository>()
		{
			Iterator<Repository> it = delegate.iterator();

			@Override
			public boolean hasNext()
			{
				return it.hasNext();
			}

			@Override
			public Repository next()
			{
				return new ElasticsearchRepositoryDecorator(it.next(), searchService);
			}

		};
	}

	@Override
	public String getName()
	{
		return delegate.getName();
	}

	@Override
	public Repository createRepository(EntityMetaData entityMeta)
	{
		Repository repo = delegate.createRepository(entityMeta);
		searchService.createMappings(entityMeta);

		return new ElasticsearchRepositoryDecorator(repo, searchService);
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		return delegate.getEntityNames();
	}

	@Override
	public Repository getRepository(String name)
	{
		Repository repo = delegate.getRepository(name);
		if (repo == null)
		{
			throw new UnknownEntityException(format("Unknown entity [%s]", name));
		}
		return new ElasticsearchRepositoryDecorator(repo, searchService);
	}

	@Override
	public Repository getRepository(EntityMetaData entityMeta)
	{
		Repository repo = delegate.getRepository(entityMeta);
		if (repo == null)
		{
			throw new UnknownEntityException(format("Unknown entity [%s]", entityMeta.getName()));
		}
		return new ElasticsearchRepositoryDecorator(repo, searchService);
	}

	/**
	 * Get undelying not indexed repository
	 * 
	 * @param name
	 * @return
	 */
	public Repository getUnderlying(String name)
	{
		return delegate.getRepository(name);
	}

	protected SearchService getSearchService()
	{
		return searchService;
	}

	@Override
	public boolean hasRepository(String name)
	{
		return delegate.hasRepository(name);
	}

	@Override
	public void deleteEntityMeta(String entityName)
	{
		delegate.deleteEntityMeta(entityName);
		getSearchService().delete(entityName);
	}

	@Override
	public void addAttribute(String entityName, AttributeMetaData attribute)
	{
		delegate.addAttribute(entityName, attribute);
		DefaultEntityMetaData meta = new DefaultEntityMetaData(delegate.getRepository(entityName).getEntityMetaData());
		meta.addAttributeMetaData(attribute);
		getSearchService().createMappings(meta);
	}

	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		delegate.deleteAttribute(entityName, attributeName);

		DefaultEntityMetaData meta = new DefaultEntityMetaData(delegate.getRepository(entityName).getEntityMetaData());

		AttributeMetaData attr = meta.getAttribute(attributeName);
		if (attr != null)
		{
			meta.removeAttributeMetaData(attr);
			getSearchService().createMappings(meta);
		}
	}

	@Override
	public void addAttributeSync(String entityName, AttributeMetaData attribute)
	{
		delegate.addAttributeSync(entityName, attribute);
		DefaultEntityMetaData meta = new DefaultEntityMetaData(delegate.getRepository(entityName).getEntityMetaData());
		meta.addAttributeMetaData(attribute);
		getSearchService().createMappings(meta);
	}

	@Override
	public void initMetaDataRepositories(ApplicationContext ctx)
	{
		delegate.initMetaDataRepositories(ctx);
	}
}
