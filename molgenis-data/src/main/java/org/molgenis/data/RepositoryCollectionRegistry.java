package org.molgenis.data;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Registry of all repository collection.
 */
@Component
public class RepositoryCollectionRegistry
{
	private final RepositoryDecoratorFactory repoDecoratorFactory;
	private final Map<String, RepositoryCollection> repoCollections;
	private String defaultRepoCollectionName;

	@Autowired
	public RepositoryCollectionRegistry(RepositoryDecoratorFactory repoDecoratorFactory)
	{
		this.repoDecoratorFactory = requireNonNull(repoDecoratorFactory);
		this.repoCollections = new HashMap<>();
	}

	public boolean hasRepositoryCollection(String repoCollectionName)
	{
		return repoCollections.containsKey(repoCollectionName);
	}

	public RepositoryCollection getRepositoryCollection(String repoCollectionName)
	{
		return repoCollections.get(repoCollectionName);
	}

	public void registerRepositoryCollection(RepositoryCollection repoCollection)
	{
		repoCollections.put(repoCollection.getName(),
				new RepositoryCollectionDecorator(repoCollection, repoDecoratorFactory));
	}

	/**
	 * Streams the {@link RepositoryCollection}s
	 */
	public Stream<RepositoryCollection> stream()
	{
		return repoCollections.values().stream();
	}

	public RepositoryCollection getDefaultRepoCollection()
	{
		return getRepositoryCollection(defaultRepoCollectionName);
	}

	public void setDefaultRepoCollectionName(String defaultRepoCollectionName)
	{
		this.defaultRepoCollectionName = defaultRepoCollectionName;
	}
}
