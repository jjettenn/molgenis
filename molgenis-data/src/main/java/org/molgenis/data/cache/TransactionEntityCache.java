package org.molgenis.data.cache;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.molgenis.MolgenisFieldTypes.FieldTypeEnum;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.support.MapEntity;
import org.molgenis.data.transaction.MolgenisTransactionListener;
import org.molgenis.data.transaction.MolgenisTransactionManager;
import org.molgenis.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Component
public class TransactionEntityCache implements MolgenisTransactionListener
{
	private static final Logger LOG = LoggerFactory.getLogger(TransactionEntityCache.class);

	// TODO wipe xrefs/mrefs
	// TODO do not cache entity objects, but entity object values to avoid cache corruption on entity manipulation, see
	// hydration/dehydration methods in this class
	private final Map<String, Cache<String, Entity>> transactionEntityCaches;

	@Autowired
	public TransactionEntityCache(MolgenisTransactionManager molgenisTransactionManager)
	{
		transactionEntityCaches = new HashMap<>();
		requireNonNull(molgenisTransactionManager).addTransactionListener(this);
	}

	public Entity cacheGet(String entityName, Object entityId)
	{
		Entity entity;
		String transactionId = getTransactionId();
		if (transactionId != null)
		{
			Cache<String, Entity> entityCache = transactionEntityCaches.get(transactionId);
			if (entityCache != null)
			{
				String key = toKey(entityName, entityId);
				entity = entityCache.getIfPresent(key);
				if (entity != null)
				{
					LOG.info("Cache hit [{}]", key);
				}
			}
			else
			{
				entity = null;
			}
		}
		else
		{
			entity = null;
		}
		return entity;
	}

	public void cachePut(String entityName, Entity entity)
	{
		String transactionId = getTransactionId();
		if (transactionId != null)
		{
			Cache<String, Entity> entityCache = transactionEntityCaches.get(transactionId);
			if (entityCache == null)
			{
				entityCache = createCache();
				transactionEntityCaches.put(transactionId, entityCache);
			}
			String key = toKey(entityName, entity.getIdValue());
			entityCache.put(key, entity);
			LOG.info("Cache put [{}]", key);
		}
	}

	public void cacheEvictAll(String entityName)
	{
		String transactionId = getTransactionId();
		if (transactionId != null)
		{
			Cache<String, Entity> entityCache = transactionEntityCaches.get(transactionId);
			if (entityCache != null)
			{
				for (Iterator<Entry<String, Entity>> it = entityCache.asMap().entrySet().iterator(); it.hasNext();)
				{
					Entry<String, Entity> entry = it.next();
					if (entry.getKey().startsWith(toKey(entityName, "")))
					{
						it.remove();
					}
				}
			}
		}
	}

	public void cacheEvict(String entityName, Object entityId)
	{
		String transactionId = getTransactionId();
		if (transactionId != null)
		{
			Cache<String, Entity> entityCache = transactionEntityCaches.get(transactionId);
			if (entityCache != null)
			{
				String key = toKey(entityName, entityId);
				entityCache.invalidate(key);
			}
		}
	}

	@Override
	public void commitTransaction(String transactionId)
	{
		transactionEntityCaches.remove(transactionId);
	}

	@Override
	public void rollbackTransaction(String transactionId)
	{
		transactionEntityCaches.remove(transactionId);
	}

	@Override
	public void transactionStarted(String transactionId)
	{
		// no op
	}

	private Cache<String, Entity> createCache()
	{
		return CacheBuilder.newBuilder().maximumSize(1000).build();
	}

	private String toKey(String entityName, Object entityId)
	{
		return entityName + '/' + entityId.toString();
	}

	private String getTransactionId()
	{
		return (String) TransactionSynchronizationManager
				.getResource(MolgenisTransactionManager.TRANSACTION_ID_RESOURCE_NAME);
	}

	/**
	 * Rebuild entity from the values representing this entity.
	 * 
	 * @param dehydratedEntity
	 * @return
	 */
	private Entity hydrateEntity(List<Pair<String, Object>> dehydratedEntity)
	{
		// TODO hydrate metadata
		MapEntity entity = new MapEntity();
		dehydratedEntity.forEach(keyValue -> {
			// TODO hydrate value based on metadata types
		});
		return entity;
	}

	/**
	 * Do not store entity in the cache since it might be updated by client code, instead store the values requires to
	 * rebuild this entity. For references to other entities only store the ids.
	 * 
	 * @param entity
	 * @return
	 */
	private List<Pair<String, Object>> dehydrateEntity(Entity entity)
	{
		// TODO dehydrate metadata
		List<Pair<String, Object>> dehydratedEntity = new ArrayList<>();
		for (AttributeMetaData attr : entity.getEntityMetaData().getAtomicAttributes())
		{
			String attrName = attr.getName();
			FieldTypeEnum attrType = attr.getDataType().getEnumType();

			Object value;
			switch (attrType)
			{
				case CATEGORICAL:
				case XREF:
				case FILE:
					Entity refEntity = entity.getEntity(attrName);
					value = refEntity != null ? refEntity.getIdValue() : null;
					break;
				case CATEGORICAL_MREF:
				case MREF:
					Iterator<Entity> refEntitiesIt = entity.getEntities(attrName).iterator();
					if (refEntitiesIt.hasNext())
					{
						List<Object> mrefValues = new ArrayList<>();
						do
						{
							Entity mrefEntity = refEntitiesIt.next();
							mrefValues.add(mrefEntity != null ? mrefEntity.getIdValue() : null);
						}
						while (refEntitiesIt.hasNext());
						value = mrefValues;
					}
					else
					{
						value = emptyList();
					}
					break;
				case DATE:
					// Store timestamp since data is mutable
					Date dateValue = entity.getDate(attrName);
					value = dateValue != null ? dateValue.getTime() : null;
					break;
				case DATE_TIME:
					// Store timestamp since data is mutable
					Timestamp dateTimeValue = entity.getTimestamp(attrName);
					value = dateTimeValue != null ? dateTimeValue.getTime() : null;
					break;
				case BOOL:
				case COMPOUND:
				case DECIMAL:
				case EMAIL:
				case ENUM:
				case HTML:
				case HYPERLINK:
				case INT:
				case LONG:
				case SCRIPT:
				case STRING:
				case TEXT:
					value = entity.get(attrName);
				default:
					throw new RuntimeException(format("Unknown attribute type [%s]", attrType));
			}
			dehydratedEntity.add(new Pair<String, Object>(attrName, value));
		}
		return dehydratedEntity;
	}
}
