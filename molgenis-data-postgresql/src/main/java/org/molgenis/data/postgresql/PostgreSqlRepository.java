package org.molgenis.data.postgresql;

import static java.lang.String.format;
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

import org.molgenis.MolgenisFieldTypes.FieldTypeEnum;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityAlreadyExistsException;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.RepositoryCapability;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.data.support.AbstractRepository;
import org.molgenis.fieldtypes.EnumField;
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
	private final MetaDataService metaDataService;

	public PostgreSqlRepository(EntityMetaData entityMeta, JdbcTemplate jdbcTemplate, DataSource dataSource,
			MetaDataService metaDataService)
	{
		this.entityMeta = requireNonNull(entityMeta);
		this.jdbcTemplate = requireNonNull(jdbcTemplate);
		this.dataSource = requireNonNull(dataSource);
		this.metaDataService = requireNonNull(metaDataService);
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

		// create enum types
		StreamSupport.stream(getEntityMetaData().getAtomicAttributes().spliterator(), false)
				.filter(this::doCreateEnumType).forEach(enumAttr -> {
					jdbcTemplate.execute(getCreateEnumTypeSql(enumAttr));
				});

		// create tables
		jdbcTemplate.execute(getCreateTableSql());

		// create junction tables
		StreamSupport.stream(getEntityMetaData().getAtomicAttributes().spliterator(), false)
				.filter(this::doCreateJunctionTable).forEach(mrefAttr -> {
					jdbcTemplate.execute(getCreateJunctionTableSql(mrefAttr));
				});
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
		return getTableName(getEntityMetaData());
	}

	private String getTableName(EntityMetaData entityMeta)
	{
		return entityMeta.getName();
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
				.filter(this::doCreateColumn).map(this::getColumnSql).collect(joining(","));
		sql.append(columnsSql);

		// add primary key constraint
		String primaryKeyConstraintSql = getPrimaryKeyContraintSql(getEntityMetaData().getIdAttribute());
		if (primaryKeyConstraintSql != null && !primaryKeyConstraintSql.isEmpty())
		{
			sql.append(',').append(primaryKeyConstraintSql);
		}

		// add unique constraints
		String uniqueContaintsSql = StreamSupport.stream(getEntityMetaData().getAtomicAttributes().spliterator(), false)
				.filter(this::doCreateUniqueConstraint).map(this::getUniqueContraintSql).collect(joining(","));
		if (uniqueContaintsSql != null && !uniqueContaintsSql.isEmpty())
		{
			sql.append(',').append(uniqueContaintsSql);
		}

		// add foreign key constraints
		String foreignKeyContraintsSql = StreamSupport
				.stream(getEntityMetaData().getAtomicAttributes().spliterator(), false)
				.filter(this::doCreateForeignKeyConstraint).map(this::getForeignKeyContraintSql).collect(joining(","));
		if (foreignKeyContraintsSql != null && !foreignKeyContraintsSql.isEmpty())
		{
			sql.append(',').append(foreignKeyContraintsSql);
		}

		sql.append(");");

		return sql.toString();
	}

	private boolean doCreateEnumType(AttributeMetaData attr)
	{
		return attr.getExpression() == null && !(attr.getDataType() instanceof EnumField);
	}

	private boolean doCreateColumn(AttributeMetaData attr)
	{
		return attr.getExpression() == null && !(attr.getDataType() instanceof MrefField);
	}

	private boolean doCreateUniqueConstraint(AttributeMetaData attr)
	{
		return attr.getExpression() == null && attr.isUnique();
	}

	private boolean doCreateForeignKeyConstraint(AttributeMetaData attr)
	{
		return attr.getExpression() == null && attr instanceof XrefField
				&& metaDataService.getBackend(attr.getRefEntity()).getName().equals(getEntityMetaData().getBackend());
	}

	private String getColumnSql(AttributeMetaData attr)
	{
		StringBuilder sql = new StringBuilder(getColumnName(attr)).append(' ').append(getDataTypeSql(attr));
		if (!attr.isNillable())
		{
			sql.append(' ').append("NOT NULL");
		}
		return sql.toString();
	}

	private String getPrimaryKeyContraintSql(AttributeMetaData attr)
	{
		return new StringBuilder("CONSTRAINT ").append("PK_").append(getTableName()).append('_')
				.append(getColumnName(attr)).append(" PRIMARY KEY (").append(getColumnName(attr)).append(')')
				.toString();
	}

	private String getForeignKeyContraintSql(AttributeMetaData attr)
	{
		return new StringBuilder("CONSTRAINT ").append("FK_").append(getTableName()).append('_')
				.append(getColumnName(attr)).append(" FOREIGN KEY (").append(getColumnName(attr))
				.append(") REFERENCES ").append(getTableName(attr.getRefEntity())).append(" (")
				.append(getColumnName(attr.getRefEntity().getIdAttribute())).append(')').toString();
	}

	private String getUniqueContraintSql(AttributeMetaData attr)
	{
		return new StringBuilder("CONSTRAINT ").append("UQ_").append(getTableName()).append('_')
				.append(getColumnName(attr)).append(" UNIQUE (").append(getColumnName(attr)).append(')').toString();
	}

	private String getDataTypeSql(AttributeMetaData attr)
	{
		FieldTypeEnum attrType = attr.getDataType().getEnumType();
		switch (attrType)
		{
			case BOOL:
				return "boolean";
			case CATEGORICAL:
			case XREF:
			case FILE:
				return "varchar(255)";
			case DATE:
				return "date";
			case DATE_TIME:
				return "timestamp";
			case DECIMAL:
				return "decimal";
			case EMAIL:
			case HTML:
			case HYPERLINK:
			case SCRIPT:
			case STRING:
			case TEXT:
				return "text";
			case ENUM:
				return getEnumTypeName(attr);
			case INT:
				return "integer";
			case LONG:
				return "bigint";
			case CATEGORICAL_MREF:
			case COMPOUND:
			case MREF:
				throw new RuntimeException(format("No data type exists for attribute type [%s]", attrType));
			default:
				throw new RuntimeException(format("Unknown attribute type [%s]", attrType));
		}
	}

	private String getEnumTypeName(AttributeMetaData attr)
	{
		return new StringBuilder(getTableName()).append('_').append(getColumnName(attr)).toString();
	}

	private String getCreateEnumTypeSql(AttributeMetaData attr)
	{
		StringBuilder strBuilder = new StringBuilder("CREATE TYPE ");
		strBuilder.append(getEnumTypeName(attr)).append(" AS ENUM (");
		strBuilder
				.append(attr.getEnumOptions().stream().map(enumOption -> "'" + enumOption + "'").collect(joining(",")));
		strBuilder.append(");");
		return strBuilder.toString();
	}

	private boolean doCreateJunctionTable(AttributeMetaData attr)
	{
		return attr.getExpression() == null && attr instanceof MrefField;
	}

	private String getCreateJunctionTableSql(AttributeMetaData mrefAttr)
	{
		// AttributeMetaData idAttribute = getEntityMetaData().getIdAttribute();
		// StringBuilder sql = new StringBuilder();
		//
		// // mysql keys cannot have TEXT value, so change it to VARCHAR when needed
		// String idAttrMysqlType = (idAttribute.getDataType() instanceof StringField ? VARCHAR
		// : idAttribute.getDataType().getMysqlType());
		//
		// String refAttrMysqlType = (att.getRefEntity().getIdAttribute().getDataType() instanceof StringField ? VARCHAR
		// : att.getRefEntity().getIdAttribute().getDataType().getMysqlType());
		//
		// sql.append(" CREATE TABLE ").append('`').append(getTableName()).append('_').append(att.getName()).append('`')
		// .append("(`order` INT,`").append(idAttribute.getName()).append('`').append(' ').append(idAttrMysqlType)
		// .append(" NOT NULL, ").append('`').append(att.getName()).append('`').append(' ')
		// .append(refAttrMysqlType).append(" NOT NULL, FOREIGN KEY (").append('`').append(idAttribute.getName())
		// .append('`').append(") REFERENCES ").append('`').append(getTableName()).append('`').append('(')
		// .append('`').append(idAttribute.getName()).append("`) ON DELETE CASCADE");
		//
		// // If the refEntity is not of type MySQL do not add a foreign key to it
		// String refEntityBackend = dataService.getMeta().getBackend(att.getRefEntity()).getName();
		// if (refEntityBackend.equalsIgnoreCase(MysqlRepositoryCollection.NAME))
		// {
		// sql.append(", FOREIGN KEY (").append('`').append(att.getName()).append('`').append(") REFERENCES ")
		// .append('`').append(getTableName(att.getRefEntity())).append('`').append('(').append('`')
		// .append(att.getRefEntity().getIdAttribute().getName()).append("`) ON DELETE CASCADE");
		// }
		//
		// sql.append(") ENGINE=InnoDB;");
		//
		// return sql.toString();
		return ""; // FIXME implement
	}
}
