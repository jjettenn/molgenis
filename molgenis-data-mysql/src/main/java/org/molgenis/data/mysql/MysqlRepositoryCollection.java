package org.molgenis.data.mysql;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.molgenis.MolgenisFieldTypes;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Package;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.UnknownEntityException;
import org.molgenis.data.i18n.DefaultLanguage;
import org.molgenis.data.i18n.I18nStringMetaData;
import org.molgenis.data.i18n.LanguageMetaData;
import org.molgenis.data.meta.AttributeMetaDataMetaData;
import org.molgenis.data.meta.DefaultPackage;
import org.molgenis.data.meta.EntityMetaDataMetaData;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.data.meta.PackageMetaData;
import org.molgenis.data.meta.TagMetaData;
import org.molgenis.data.support.SystemEntityMetaData;
import org.molgenis.util.ApplicationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public abstract class MysqlRepositoryCollection implements RepositoryCollection
{
	private static final Logger LOG = LoggerFactory.getLogger(MysqlRepositoryCollection.class);

	public static final String NAME = "MySQL";
	// private final Map<String, MysqlRepository> repositories = new LinkedHashMap<>();

	private final DataSource dataSource;
	private final MySqlEntityFactory mySqlEntityFactory;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;
	private JdbcTemplate jdbcTemplate;
	private String schemaName;

	public MysqlRepositoryCollection(DataSource dataSource, MySqlEntityFactory mySqlEntityFactory,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry)
	{
		this.dataSource = requireNonNull(dataSource);
		this.mySqlEntityFactory = requireNonNull(mySqlEntityFactory);
		this.systemEntityMetaDataRegistry = requireNonNull(systemEntityMetaDataRegistry);
	}

	@Override
	public void initMetaDataRepositories(ApplicationContext ctx)
	{
		// determine language
		List<String> languageCodes;
		if (!tableExists(LanguageMetaData.ENTITY_NAME))
		{
			LanguageMetaData languageMetaData = ctx.getBean(LanguageMetaData.class);
			createRepository(languageMetaData);

			// insert default language
			DefaultLanguage defaultLanguage = ctx.getBean(DefaultLanguage.class);
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("INSERT INTO "); // FIXME data not indexed
			sqlBuilder.append(LanguageMetaData.ENTITY_NAME);
			sqlBuilder.append(" (");
			sqlBuilder.append(LanguageMetaData.CODE).append(',');
			sqlBuilder.append(LanguageMetaData.NAME);
			sqlBuilder.append(") VALUES (?,?)");

			getJdbcTemplate().update(sqlBuilder.toString(), new Object[]
			{ defaultLanguage.getCode(), defaultLanguage.getName() });

			languageCodes = singletonList(defaultLanguage.getCode());
		}
		else
		{
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("SELECT ");
			sqlBuilder.append(LanguageMetaData.CODE);
			sqlBuilder.append(" FROM ");
			sqlBuilder.append(LanguageMetaData.ENTITY_NAME);
			languageCodes = getJdbcTemplate().query(sqlBuilder.toString(), new RowMapper<String>()
			{
				@Override
				public String mapRow(ResultSet rs, int rowNum) throws SQLException
				{
					return rs.getString(LanguageMetaData.CODE);
				}
			});
		}

		// update system entity meta data beans with information from backend
		Map<String, SystemEntityMetaData> entityMetaBeans = ctx.getBeansOfType(SystemEntityMetaData.class);
		entityMetaBeans.values().stream().forEach(entityMetaBean -> {
			updateSystemEntityMeta(ctx, entityMetaBean, languageCodes);
		});

		// create tables if not exists
		if (!tableExists(TagMetaData.ENTITY_NAME))
		{
			TagMetaData tagMetaData = ctx.getBean(TagMetaData.class);
			createRepository(tagMetaData);
		}

		if (!tableExists(PackageMetaData.ENTITY_NAME))
		{
			PackageMetaData packageMetaData = ctx.getBean(PackageMetaData.class);
			createRepository(packageMetaData);

			// insert default package
			Map<String, Package> packageBeans = ctx.getBeansOfType(Package.class);
			packageBeans.values().forEach(packageBean -> { // FIXME data not indexed
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("INSERT INTO ");
				sqlBuilder.append(PackageMetaData.ENTITY_NAME);
				sqlBuilder.append(" (");
				sqlBuilder.append(PackageMetaData.FULL_NAME).append(',');
				sqlBuilder.append(PackageMetaData.SIMPLE_NAME).append(',');
				sqlBuilder.append(PackageMetaData.DESCRIPTION).append(',');
				sqlBuilder.append(PackageMetaData.PARENT);
				sqlBuilder.append(") VALUES (?,?,?,?)");

				getJdbcTemplate().update(sqlBuilder.toString(), new Object[]
				{ packageBean.getName(), packageBean.getSimpleName(), packageBean.getDescription(), null });
			});
		}

		if (!tableExists(AttributeMetaDataMetaData.ENTITY_NAME))
		{
			AttributeMetaDataMetaData attrMetaDataMetaData = ctx.getBean(AttributeMetaDataMetaData.class);
			createRepository(attrMetaDataMetaData);
		}

		if (!tableExists(EntityMetaDataMetaData.ENTITY_NAME))
		{
			EntityMetaDataMetaData entityMetaDataMetaData = ctx.getBean(EntityMetaDataMetaData.class);
			createRepository(entityMetaDataMetaData);
		}

		// create rows for attrs, entities, tags, packages in entities table

	}

	private void updateSystemEntityMeta(ApplicationContext ctx, SystemEntityMetaData entityMeta,
			List<String> languageCodes)
	{
		// set default repository collection if undefined
		if (entityMeta.getBackend() == null)
		{
			entityMeta.setBackend(NAME);
		}

		// set default package if undefined
		DefaultPackage defaultPackage = ctx.getBean(DefaultPackage.class);
		if (entityMeta.getPackage() == null)
		{
			entityMeta.setPackage(defaultPackage);
		}

		// set language attributes
		languageCodes.forEach(languageCode -> {
			// update attribute meta data
			AttributeMetaDataMetaData attrMetaDataMetaData = ctx.getBean(AttributeMetaDataMetaData.class);
			attrMetaDataMetaData.addAttribute(AttributeMetaDataMetaData.LABEL + languageCode);
			attrMetaDataMetaData.addAttribute(AttributeMetaDataMetaData.DESCRIPTION + languageCode)
					.setDataType(MolgenisFieldTypes.TEXT);

			// update entity meta data
			EntityMetaDataMetaData entityMetaDataMetaData = ctx.getBean(EntityMetaDataMetaData.class);
			entityMetaDataMetaData.addAttribute(EntityMetaDataMetaData.LABEL + languageCode);
			entityMetaDataMetaData.addAttribute(EntityMetaDataMetaData.DESCRIPTION + languageCode)
					.setDataType(MolgenisFieldTypes.TEXT);

			// update I18N string
			I18nStringMetaData i18nStringMetaData = ctx.getBean(I18nStringMetaData.class);
			i18nStringMetaData.addLanguage(languageCode);
		});

		// inject attribute identifiers
		// TODO
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	// private void createRepositoryTableIfNotExists()
	// {
	// StringBuilder sqlBuilder = new StringBuilder();
	// sqlBuilder.append("CREATE TABLE IF NOT EXISTS Repository (");
	// sqlBuilder.append(RepositoryMetaData.ID);
	// sqlBuilder.append(" text, ");
	// sqlBuilder.append(RepositoryMetaData.COLLECTION);
	// sqlBuilder.append(" varchar(255))");
	// getJdbcTemplate().execute(sqlBuilder.toString());
	// }

	@Override
	public Repository createRepository(EntityMetaData entityMeta)
	{
		// for now: do upsert
		// LOG.info("Adding meta entity [{}]", entityMeta.getName());

		MysqlRepository repository = createMysqlRepository();
		repository.setMetaData(entityMeta);
		repository.create();

		// 1. add row to repository table in repository collection
		// Map<String, Object> resultMap = jdbcTemplate
		// .queryForMap("SELECT to_regclass('" + getSchemaName() + '.' + entityMeta.getName() + "');");
		// if (!resultMap.isEmpty())
		// {
		// StringBuilder sqlBuilder = new StringBuilder();
		// sqlBuilder.append("INSERT INTO Repository (");
		// sqlBuilder.append(RepositoryMetaData.ID);
		// sqlBuilder.append(',');
		// sqlBuilder.append(RepositoryMetaData.COLLECTION);
		// sqlBuilder.append(") VALUES (?, ?)");
		// // TODO use result map to check if exists
		// jdbcTemplate.update(sqlBuilder.toString(), entityMeta.getName(), entityMeta.getBackend());
		// }
		// null rather than throwing an error if the name is not found
		// MysqlRepository repository = createMysqlRepository();
		// repository.setMetaData(entityMeta);
		// if (!RepositoryMetaData.ENTITY_NAME.equals(entityMeta.getName()))
		// {
		// repository.create();
		// }
		// repositories.put(entityMeta.getName(), repository);

		return repository;
	}

	@Override
	public Iterable<String> getEntityNames()
	{
		if (tableExists("entities"))
		{
			// FIXME should only do query if this is the default backend, else forward request to default backend
			// TODO security
			// TODO batching
			// TODO duplicate code with MysqlRepository
			return new Iterable<String>()
			{
				@Override
				public Iterator<String> iterator()
				{
					return getJdbcTemplate().query("SELECT fullName FROM entities WHERE backend = ?", new Object[]
					{ NAME }, new RowMapper<String>()
					{

						@Override
						public String mapRow(ResultSet rs, int rowNum) throws SQLException
						{
							return rs.getString(EntityMetaDataMetaData.FULL_NAME);
						}
					}).stream().iterator();
				}
			};
		}
		else
		{
			return emptyList();
		}
	}

	private boolean tableExists(String tableName)
	{
		Connection conn = null;
		try
		{
			conn = dataSource.getConnection();
			DatabaseMetaData dbm = conn.getMetaData();
			ResultSet tables = dbm.getTables(null, null, tableName, null);
			return tables.next();
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		finally
		{
			try
			{
				conn.close();
			}
			catch (Exception e2)
			{
				e2.printStackTrace();
			}
		}
	}

	private Stream<String> getRepositoryNames()
	{
		// FIXME should only do query if this is the default backend, else forward request to default backend
		// TODO security
		// TODO batching
		// TODO duplicate code with MysqlRepository
		return getJdbcTemplate().query("SELECT fullName FROM entities WHERE backend = ? AND abstract = ?", new Object[]
		{ NAME, Boolean.FALSE }, new RowMapper<String>()
		{

			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				return rs.getString(EntityMetaDataMetaData.FULL_NAME);
			}
		}).stream();
	}

	@Override
	public Repository getRepository(String name)
	{
		// TODO exists check? exception handling?
		return createRepository(name);
	}

	@Override
	public Repository getRepository(EntityMetaData entityMeta)
	{
		MysqlRepository mysqlRepository = new MysqlRepository(mySqlEntityFactory.getDataService(), mySqlEntityFactory,
				dataSource, null);
		mysqlRepository.setMetaData(entityMeta);
		return mysqlRepository;
	}

	@Override
	public boolean hasRepository(String name)
	{
		// EntityMetaData systemEntity = systemEntityMetaDataRegistry.getSystemEntity(name);
		// if (systemEntity != null)
		// {
		// return systemEntity.getBackend().equals(NAME);
		// }
		// else
		// {
		Map<String, Object> queryForMap = getJdbcTemplate().queryForMap(
				"select exists(select 1 from entities where fullName = ? and abstract = ?)", name, Boolean.FALSE);
		return (Boolean) queryForMap.get("exists");
		// }
	}

	private Repository createRepository(String name)
	{
		// FIXME get rid of hack
		MetaDataService metaDataService = ApplicationContextProvider.getApplicationContext()
				.getBean(MetaDataService.class);
		EntityMetaData entityMeta = metaDataService.getEntityMetaData(name);
		if (entityMeta == null)
		{
			throw new UnknownEntityException(format("unknown entity [%s]", name));
		}

		return getRepository(entityMeta);
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