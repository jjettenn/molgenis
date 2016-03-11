package org.molgenis.data.mysql;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.ManageableRepositoryCollection;
import org.molgenis.data.Repository;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.data.meta.RepositoryMetaData;
import org.molgenis.util.ApplicationContextProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public abstract class MysqlRepositoryCollection implements ManageableRepositoryCollection
{
	public static final String NAME = "MySQL";
	// private final Map<String, MysqlRepository> repositories = new LinkedHashMap<>();

	private final DataSource dataSource;
	private final MySqlEntityFactory mySqlEntityFactory;
	private JdbcTemplate jdbcTemplate;
	private String schemaName;

	public MysqlRepositoryCollection(DataSource dataSource, MySqlEntityFactory mySqlEntityFactory)
	{
		this.dataSource = requireNonNull(dataSource);
		this.mySqlEntityFactory = requireNonNull(mySqlEntityFactory);
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	private void createRepositoryTableIfNotExists()
	{
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE TABLE IF NOT EXISTS Repository (");
		sqlBuilder.append(RepositoryMetaData.ID);
		sqlBuilder.append(" text, ");
		sqlBuilder.append(RepositoryMetaData.COLLECTION);
		sqlBuilder.append(" varchar(255))");
		getJdbcTemplate().execute(sqlBuilder.toString());
	}

	@Override
	public Repository addEntityMeta(EntityMetaData entityMeta)
	{
		createRepositoryTableIfNotExists();

		// 1. add row to repository table in repository collection
		// Map<String, Object> resultMap = jdbcTemplate
		// .queryForMap("SELECT to_regclass('" + getSchemaName() + '.' + entityMeta.getName() + "');");
		// if (!resultMap.isEmpty())
		// {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("INSERT INTO Repository (");
		sqlBuilder.append(RepositoryMetaData.ID);
		sqlBuilder.append(',');
		sqlBuilder.append(RepositoryMetaData.COLLECTION);
		sqlBuilder.append(") VALUES (?, ?)");
		// TODO use result map to check if exists
		jdbcTemplate.update(sqlBuilder.toString(), entityMeta.getName(), entityMeta.getBackend());
		// }
		// null rather than throwing an error if the name is not found
		MysqlRepository repository = createMysqlRepository();
		repository.setMetaData(entityMeta);
		if (!RepositoryMetaData.ENTITY_NAME.equals(entityMeta.getName()))
		{
			repository.create();
		}
		// repositories.put(entityMeta.getName(), repository);

		return repository;
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		// TODO add abstract entities?
		return new Iterable<String>()
		{
			@Override
			public Iterator<String> iterator()
			{
				return getRepositoryNames().iterator();
			}
		};
	}

	private Stream<String> getRepositoryNames()
	{
		createRepositoryTableIfNotExists();

		// TODO batching
		// TODO duplicate code with MysqlRepository
		return getJdbcTemplate().query("SELECT id FROM Repository WHERE collection = ?", new Object[]
		{ NAME }, new RowMapper<String>()
		{

			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				return rs.getString(RepositoryMetaData.ID);
			}
		}).stream();
	}

	@Override
	public Repository getRepository(String name)
	{
		// TODO exists check? exception handling?
		return createRepository(name);
	}

	private Repository createRepository(String name)
	{

		MysqlRepository mysqlRepository = new MysqlRepository(mySqlEntityFactory.getDataService(), mySqlEntityFactory,
				dataSource, null);
		mysqlRepository.setMetaData(ApplicationContextProvider.getApplicationContext().getBean(MetaDataService.class)
				.getEntityMetaData(name)); // FIXME get rid of hack
		return mysqlRepository;
	}

	@Override
	public Iterator<Repository> iterator()
	{
		return getRepositoryNames().map(this::createRepository).iterator();
		// return Iterators.transform(repositories.values().iterator(), new Function<MysqlRepository, Repository>()
		// {
		// @Override
		// public Repository apply(MysqlRepository repo)
		// {
		// return repo;
		// }
		// });
	}

	@Override
	public void deleteEntityMeta(String entityName)
	{
		// TODO implement
		// MysqlRepository repo = repositories.get(entityName);
		// if (repo != null)
		// {
		// repo.drop();
		// repositories.remove(entityName);
		// }
	}

	@Override
	public void addAttribute(String entityName, AttributeMetaData attribute)
	{
		// TODO implement
		// MysqlRepository repo = repositories.get(entityName);
		// if (repo == null) throw new UnknownEntityException(String.format("Unknown entity '%s'", entityName));
		// repo.addAttribute(attribute);
	}

	@Override
	public void deleteAttribute(String entityName, String attributeName)
	{
		// TODO implement
		// MysqlRepository repo = repositories.get(entityName);
		// if (repo == null) throw new UnknownEntityException(String.format("Unknown entity '%s'", entityName));
		// repo.dropAttribute(attributeName);
	}

	@Override
	public void addAttributeSync(String entityName, AttributeMetaData attribute)
	{
		// TODO implement
		// MysqlRepository repo = repositories.get(entityName);
		// if (repo == null) throw new UnknownEntityException(String.format("Unknown entity '%s'", entityName));
		// repo.addAttributeSync(attribute);
	}

	/**
	 * Return a spring managed prototype bean
	 */
	protected abstract MysqlRepository createMysqlRepository();

	private synchronized JdbcTemplate getJdbcTemplate()
	{
		if (jdbcTemplate == null)
		{
			jdbcTemplate = new JdbcTemplate(dataSource);
		}
		return jdbcTemplate;
	}

	private synchronized String getSchemaName()
	{
		if (schemaName == null)
		{
			Connection connection = null;
			try
			{
				connection = dataSource.getConnection();
				schemaName = connection.getCatalog();
			}
			catch (SQLException e)
			{
				throw new RuntimeException(e);
			}
			finally
			{
				if (connection != null)
				{
					try
					{
						connection.close();
					}
					catch (SQLException e)
					{
						throw new RuntimeException(e);
					}
				}
			}
		}
		return schemaName;
	}
}