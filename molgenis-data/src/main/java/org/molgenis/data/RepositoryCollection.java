package org.molgenis.data;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationContext;

// TODO add capabilities
/**
 * Repository collection
 */
public interface RepositoryCollection extends Iterable<Repository>
{
	void initMetaDataRepositories(ApplicationContext ctx);

	/**
	 * @return the name of this backend
	 */
	String getName();

	/**
	 * Streams the {@link Repository}s
	 */
	default Stream<Repository> stream()
	{
		return StreamSupport.stream(spliterator(), false);
	}

	/**
	 * Create and add a new CrudRepository for an EntityMetaData
	 * 
	 * @param entityMeta
	 * @return
	 */
	Repository createRepository(EntityMetaData entityMeta);

	/**
	 * Get names of all the entities in this source
	 * 
	 * @return
	 */
	Iterable<String> getEntityNames();

	/**
	 * Get a repository by entity name
	 * 
	 * @param name
	 * @return
	 * @throws UnknownEntityException
	 * @deprecated
	 */
	@Deprecated
	Repository getRepository(String name);

	/**
	 * Get a repository for the given entity meta data
	 * 
	 * @param entityMeta
	 * @return
	 */
	Repository getRepository(EntityMetaData entityMeta);

	/**
	 * Check if a repository exists by entity name
	 * 
	 * @param name
	 * @return
	 */
	boolean hasRepository(String name);

	/**
	 * Removes an entity definition from this ManageableCrudRepositoryCollection
	 * 
	 * @param entityName
	 */
	void deleteEntityMeta(String entityName);

	/**
	 * Adds an Attribute to an EntityMeta
	 * 
	 * @param entityName
	 * @param attribute
	 */
	void addAttribute(String entityName, AttributeMetaData attribute);

	/**
	 * Removes an attribute from an entity
	 * 
	 * @param entityName
	 * @param attributeName
	 */
	void deleteAttribute(String entityName, String attributeName);

	void addAttributeSync(String entityName, AttributeMetaData attribute);
}
