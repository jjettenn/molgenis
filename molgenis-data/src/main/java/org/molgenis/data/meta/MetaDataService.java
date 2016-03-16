package org.molgenis.data.meta;

import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.Package;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryCollection;

public interface MetaDataService extends Iterable<RepositoryCollection>
{
	/**
	 * Streams the {@link RepositoryCollection}s
	 */
	default Stream<RepositoryCollection> stream()
	{
		return StreamSupport.stream(spliterator(), false);
	}

	/**
	 * Creates a {@link Repository} in the {@link RepositoryCollection} defined by entity meta data
	 * 
	 * @param entityMeta
	 */
	void createRepository(EntityMetaData entityMeta);

	/**
	 * Returns the {@link Repository} for the given entity meta data
	 * 
	 * @param entityMeta
	 * @return
	 */
	Repository getRepository(EntityMetaData entityMeta);

	/**
	 * Returns the {@link Repository} for the given entity meta data name
	 * 
	 * @param entityName
	 * @return
	 */
	Repository getRepository(String entityName);

	/**
	 * Get a backend by name or null if it does not exists
	 * 
	 * @param name
	 * @return
	 */
	RepositoryCollection getBackend(String name);

	/**
	 * Get the backend the EntityMetaData belongs to
	 * 
	 * @param emd
	 * @return
	 */
	RepositoryCollection getBackend(EntityMetaData emd);

	/**
	 * Get the default backend to store entities
	 * 
	 * @return
	 */
	RepositoryCollection getDefaultBackend();

	/**
	 * Get the backend that stores entity meta data
	 * 
	 * @return
	 */
	RepositoryCollection getMetaDataBackend();

	/**
	 * Get all packages
	 * 
	 * @return List of Package
	 */
	public List<Package> getPackages();

	/**
	 * Lists all root packages.
	 * 
	 * @return Iterable of all root Packages
	 */
	Iterable<Package> getRootPackages();

	/**
	 * Returns the default package
	 * 
	 * @return
	 */
	Package getDefaultPackage();

	/**
	 * Retrieves a package with a given name.
	 * 
	 * @param name
	 *            the name of the Package to retrieve
	 * @return the Package, or null if the package does not exist.
	 */
	Package getPackage(String name);

	/**
	 * Adds a new Package
	 * 
	 * @param package_
	 */
	void addPackage(Package package_);

	/**
	 * Adds a package if the package does not exist, else updates the package
	 * 
	 * @param package_
	 */
	void upsertPackage(Package package_);

	/**
	 * Gets the entity meta data for a given entity.
	 * 
	 * @param name
	 *            the fullyQualifiedName of the entity
	 * @return EntityMetaData of the entity, or null if the entity does not exist
	 */
	EntityMetaData getEntityMetaData(String name);

	/**
	 * @deprecated Rebuilds all meta data chaches
	 * 
	 *             TODO remove
	 */
	@Deprecated
	void refreshCaches();

	Iterable<EntityMetaData> getEntityMetaDatas();

	/**
	 * Adds new EntityMeta with new AttributeMetaData and creates a new Repository
	 * 
	 * @param entityMeta
	 * @return
	 */
	Repository addEntityMeta(EntityMetaData entityMeta);

	/**
	 * Adds an entity if the entity does not exist, else updates the entity
	 * 
	 * @param entityMeta
	 */
	void upsertEntityMeta(EntityMetaData entityMeta);

	/**
	 * Deletes an EntityMeta
	 */
	void deleteEntityMeta(String entityName);

	/**
	 * Deletes a list of EntityMetaData
	 * 
	 * @param entities
	 */
	void delete(List<EntityMetaData> entities);

	/**
	 * Updates EntityMeta
	 * 
	 * @param entityMeta
	 * @return added attributes
	 * 
	 *         FIXME remove return value or change it to ChangeSet with all changes
	 */
	List<AttributeMetaData> updateEntityMeta(EntityMetaData entityMeta);

	/**
	 * Adds an Attribute to an EntityMeta
	 * 
	 * @param entityName
	 * @param attribute
	 */
	void addAttribute(String entityName, AttributeMetaData attribute);

	/**
	 * Deletes an Attribute
	 * 
	 * @param entityName
	 * @param attributeName
	 */
	void deleteAttribute(String entityName, String attributeName);

	/**
	 * Has backend will check if the requested backend already exists and is registered.
	 * 
	 * @param backendName
	 * @return
	 */
	boolean hasBackend(String backendName);

	/**
	 * Returns whether this repository is a repository that contains meta data (e.g. entities, attributes, packages)
	 * 
	 * @param entityName
	 * @return
	 */
	boolean isMetaRepository(String entityName);
}
