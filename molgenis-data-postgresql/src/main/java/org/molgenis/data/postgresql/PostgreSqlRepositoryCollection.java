package org.molgenis.data.postgresql;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.sql.DataSource;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.DataService;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryAlreadyExistsException;
import org.molgenis.data.meta.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PostgreSqlRepositoryCollection implements RepositoryCollection
{
	static final String NAME = "PostgreSQL";

	private final JdbcTemplate jdbcTemplate;
	private final DataSource dataSource;
	private final MetaDataService metaDataService;
	private final Map<String, PostgreSqlRepository> repositories;

	@Autowired
	public PostgreSqlRepositoryCollection(JdbcTemplate jdbcTemplate, DataSource dataSource,
			MetaDataService metaDataService, DataService dataService)
	{
		this.jdbcTemplate = requireNonNull(jdbcTemplate);
		this.dataSource = requireNonNull(dataSource);
		this.metaDataService = requireNonNull(metaDataService);
		this.repositories = new HashMap<>();
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	public Repository addEntityMeta(EntityMetaData entityMeta)
	{
		String repoName = entityMeta.getName();
		if (hasRepository(repoName))
		{
			throw new RepositoryAlreadyExistsException(repoName);
		}
		PostgreSqlRepository postgreSqlRepository = createRepository(entityMeta);
		repositories.put(repoName, postgreSqlRepository);
		return postgreSqlRepository;
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		return repositories.keySet();
	}

	@Override
	public PostgreSqlRepository getRepository(String name)
	{
		return repositories.get(name);
	}

	@Override
	public boolean hasRepository(String name)
	{
		return repositories.containsKey(name);
	}

	@Override
	public Iterator<Repository> iterator()
	{
		return repositories.values().stream().map(repo -> (Repository) repo).iterator();
	}

	@Override
	public void deleteEntityMeta(String entityName)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void addAttribute(String entityName, AttributeMetaData attribute)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void addAttributeSync(String entityName, AttributeMetaData attribute)
	{
		throw new UnsupportedOperationException();
	}

	private PostgreSqlRepository createRepository(EntityMetaData entityMeta)
	{
		PostgreSqlRepository postgreSqlRepository = new PostgreSqlRepository(entityMeta, jdbcTemplate, dataSource,
				metaDataService);
		return postgreSqlRepository;
	}
}
