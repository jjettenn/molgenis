package org.molgenis.data.cache;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.molgenis.data.Entity;
import org.molgenis.data.transaction.MolgenisTransactionListener;
import org.molgenis.data.transaction.MolgenisTransactionManager;
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

	private final Map<String, Cache<String, Entity>> transactionCache; // TODO wipe xrefs/mrefs

	@Autowired
	public TransactionEntityCache(MolgenisTransactionManager molgenisTransactionManager)
	{
		transactionCache = new HashMap<>();
		requireNonNull(molgenisTransactionManager).addTransactionListener(this);
	}

	public Entity cacheGet(String entityName, Object entityId)
	{
		Entity entity;
		String transactionId = getTransactionId();
		if (transactionId != null)
		{
			Cache<String, Entity> entityCache = transactionCache.get(transactionId);
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
			Cache<String, Entity> entityCache = transactionCache.get(transactionId);
			if (entityCache == null)
			{
				entityCache = createCache();
				transactionCache.put(transactionId, entityCache);
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
			Cache<String, Entity> entityCache = transactionCache.get(transactionId);
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
			Cache<String, Entity> entityCache = transactionCache.get(transactionId);
			if (entityCache != null)
			{
				String key = toKey(entityName, entityId);
				entityCache.invalidate(key);
			}
		}
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

	@Override
	public void commitTransaction(String transactionId)
	{
		transactionCache.remove(transactionId);
	}

	@Override
	public void rollbackTransaction(String transactionId)
	{
		transactionCache.remove(transactionId);
	}

	@Override
	public void transactionStarted(String transactionId)
	{
		// no op
	}
}
