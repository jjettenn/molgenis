package org.molgenis.data.postgresql;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityAlreadyExistsException;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.RepositoryCapability;
import org.molgenis.data.support.AbstractRepository;
import org.molgenis.fieldtypes.MrefField;
import org.molgenis.fieldtypes.XrefField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class PostgreSqlRepository extends AbstractRepository
{
	private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlRepository.class);

	private static final Set<RepositoryCapability> REPO_CAPABILITIES;

	static
	{
		REPO_CAPABILITIES = new HashSet<>();
		REPO_CAPABILITIES.add(RepositoryCapability.MANAGABLE);
		REPO_CAPABILITIES.add(RepositoryCapability.QUERYABLE);
		REPO_CAPABILITIES.add(RepositoryCapability.WRITABLE);
	}

	private final EntityMetaData entityMeta;
	private final JdbcTemplate jdbcTemplate;
	private final DataSource dataSource;

	public PostgreSqlRepository(EntityMetaData entityMeta, JdbcTemplate jdbcTemplate, DataSource dataSource)
	{
		this.entityMeta = requireNonNull(entityMeta);
		this.jdbcTemplate = requireNonNull(jdbcTemplate);
		this.dataSource = requireNonNull(dataSource);
	}

	@Override
	public Set<RepositoryCapability> getCapabilities()
	{
		return Collections.unmodifiableSet(REPO_CAPABILITIES);
	}

	@Override
	public EntityMetaData getEntityMetaData()
	{
		return entityMeta;
	}

	@Override
	public void create()
	{
		if (tableExists())
		{
			throw new EntityAlreadyExistsException(getEntityMetaData().getName());
		}

		jdbcTemplate.execute(getCreateTableSql());

		// create cross-reference tables
	}

	@Override
	public void add(Entity entity)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Entity> iterator()
	{
		// TODO Auto-generated method stub
		return null;
	}

	private boolean tableExists()
	{
		Connection connection = null;
		try
		{
			connection = dataSource.getConnection();
			DatabaseMetaData dbm = connection.getMetaData();
			ResultSet tables = dbm.getTables(null, null, getTableName(), null);
			return tables.next();
		}
		catch (SQLException e)
		{
			LOG.error("", e);
			throw new RuntimeException(e);
		}
		finally
		{
			try
			{
				connection.close();
			}
			catch (SQLException e)
			{
				LOG.error("", e);
				throw new RuntimeException(e);
			}
		}
	}

	private String getTableName()
	{
		return getEntityMetaData().getName();
	}

	private String getColumnName(AttributeMetaData attr)
	{
		return attr.getName();
	}

	private String getCreateTableSql()
	{
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(getTableName()).append('(');

		// add columns
		String columnsSql = StreamSupport.stream(getEntityMetaData().getAtomicAttributes().spliterator(), false)
				.filter(this::doCreateColumn).map(this::getCreateColumnSql).collect(joining(","));

		AttributeMetaData idAttr = getEntityMetaData().getIdAttribute();
		// FIXME continue

		sql.append(columnsSql).append(");");

		// primary key is first attribute unless otherwise indicated
		AttributeMetaData idAttribute = getEntityMetaData().getIdAttribute();

		if (idAttribute == null) throw new MolgenisDataException("Missing idAttribute for entity [" + getName() + "]");

		if (idAttribute.getDataType() instanceof XrefField || idAttribute.getDataType() instanceof MrefField)
			throw new RuntimeException(
					"primary key(" + getTableName() + "." + idAttribute.getName() + ") cannot be XREF or MREF");

		if (idAttribute.isNillable() == true) throw new RuntimeException(
				"idAttribute (" + getTableName() + "." + idAttribute.getName() + ") should not be nillable");

		sql.append("PRIMARY KEY (").append('`').append(getEntityMetaData().getIdAttribute().getName()).append('`')
				.append(')');

		// close
		sql.append(") ENGINE=InnoDB;");

		if (LOG.isTraceEnabled())
		{
			LOG.trace("sql: " + sql);
		}

		return sql.toString();
	}

	private boolean doCreateColumn(AttributeMetaData attr)
	{
		return attr.getExpression() == null && !(attr.getDataType() instanceof MrefField);
	}

	private String getCreateColumnSql(AttributeMetaData attr)
	{
		StringBuilder sql = new StringBuilder();
		// sql.append(str)
		return sql.toString();
		// sql.append('`').append(att.getName()).append('`').append(' ');
		// // xref adopt type of the identifier of referenced entity
		// if (att.getDataType() instanceof XrefField)
		// {
		// // mysql keys can not be of type TEXT, so don't adopt the field type of a referenced entity when it is
		// // of fieldtype STRING
		// if (att.getRefEntity().getIdAttribute().getDataType() instanceof StringField)
		// {
		// sql.append(VARCHAR);
		// }
		// else
		// {
		// sql.append(att.getRefEntity().getIdAttribute().getDataType().getMysqlType());
		// }
		// }
		// else
		// {
		// if (att.equals(getEntityMetaData().getIdAttribute()) && att.getDataType() instanceof StringField)
		// {
		// // id attributes can not be of type TEXT so we'll change it to VARCHAR
		// sql.append(VARCHAR);
		// }
		// else if (att.isUnique() && att.getDataType() instanceof StringField)
		// {
		// // mysql TEXT fields cannot be UNIQUE, so use VARCHAR instead
		// sql.append(VARCHAR);
		// }
		// else
		// {
		// sql.append(att.getDataType().getMysqlType());
		// }
		// }
		// // not null
		// if (!att.isNillable() && !EntityUtils.doesExtend(metaData, "Questionnaire")
		// && (att.getVisibleExpression() == null))
		// {
		// sql.append(" NOT NULL");
		// }
		// // int + auto = auto_increment
		// if (att.getDataType().equals(MolgenisFieldTypes.INT) && att.isAuto())
		// {
		// sql.append(" AUTO_INCREMENT");
		// }
	}
}
