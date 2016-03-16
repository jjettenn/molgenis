package org.molgenis.data;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationContext;

class RepositoryCollectionDecorator implements RepositoryCollection
{
	private final RepositoryCollection repoCollection;
	private final RepositoryDecoratorFactory repoDecoratorFactory;

	public RepositoryCollectionDecorator(RepositoryCollection repoCollection,
			RepositoryDecoratorFactory repoDecoratorFactory)
	{
		this.repoCollection = requireNonNull(repoCollection);
		this.repoDecoratorFactory = requireNonNull(repoDecoratorFactory);
	}

	@Override
	public Iterator<Repository> iterator()
	{
		Iterable<Repository> iterable = () -> repoCollection.iterator();
		return StreamSupport.stream(iterable.spliterator(), false).map(repoDecoratorFactory::createDecoratedRepository)
				.iterator();
	}

	@Override
	public String getName()
	{
		return repoCollection.getName();
	}

	@Override
	public Repository createRepository(EntityMetaData entityMeta)
	{
		Repository undecoratedRepo = repoCollection.createRepository(entityMeta);
		return repoDecoratorFactory.createDecoratedRepository(undecoratedRepo);
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		return repoCollection.getEntityNames();
	}

	@SuppressWarnings("deprecation")
	@Override
	public Repository getRepository(String name)
	{
		Repository undecoratedRepo = repoCollection.getRepository(name);
		return repoDecoratorFactory.createDecoratedRepository(undecoratedRepo);
	}

	@Override
	public Repository getRepository(EntityMetaData entityMeta)
	{
		Repository undecoratedRepo = repoCollection.getRepository(entityMeta);
		return repoDecoratorFactory.createDecoratedRepository(undecoratedRepo);
	}

	@Override
	public boolean hasRepository(String name)
	{
		return repoCollection.hasRepository(name);
	}

	@Override
	public void deleteEntityMeta(String entityName)
	{
		repoCollection.deleteEntityMeta(entityName);
	}

	@Override
	public void addAttribute(String entityName, AttributeMetaData attribute)
	{
		repoCollection.addAttribute(entityName, attribute);
	}

	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		repoCollection.deleteAttribute(entityName, attributeName);
	}

	@Override
	public void addAttributeSync(String entityName, AttributeMetaData attribute)
	{
		repoCollection.addAttributeSync(entityName, attribute);
	}

	@Override
	public void initMetaDataRepositories(ApplicationContext ctx)
	{
		repoCollection.initMetaDataRepositories(ctx);
	}
}