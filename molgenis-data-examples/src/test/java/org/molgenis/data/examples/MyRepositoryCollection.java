package org.molgenis.data.examples;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;
import org.springframework.context.ApplicationContext;

public class MyRepositoryCollection implements RepositoryCollection
{
	private final Map<String, Repository> repositories = new LinkedHashMap<>();

	@Override
	public Iterator<Repository> iterator()
	{
		return repositories.values().iterator();
	}

	@Override
	public String getName()
	{
		return "MyRepos";
	}

	@Override
	public Repository createRepository(EntityMetaData entityMeta)
	{
		Repository repo = new MyRepository(entityMeta);
		repositories.put(entityMeta.getName(), repo);

		return repo;
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		return repositories.keySet();
	}

	@Override
	public Repository getRepository(String name)
	{
		return repositories.get(name);
	}

	@Override
	public boolean hasRepository(String name)
	{
		if (null == name) return false;
		Iterator<String> entityNames = getEntityNames().iterator();
		while (entityNames.hasNext())
		{
			if (entityNames.next().equals(name)) return true;
		}
		return false;
	}

	@Override
	public void initMetaDataRepositories(ApplicationContext ctx)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public Repository getRepository(EntityMetaData entityMeta)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteEntityMeta(String entityName)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void addAttribute(String entityName, AttributeMetaData attribute)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void addAttributeSync(String entityName, AttributeMetaData attribute)
	{
		// TODO Auto-generated method stub

	}

}
