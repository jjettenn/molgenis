package org.molgenis.data.meta;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.IDENTIFIER;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.PARTS;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.ManageableRepositoryCollection;
import org.molgenis.data.Repository;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.UuidGenerator;

import com.google.common.collect.Iterables;

/**
 * Helper class around the {@link AttributeMetaDataMetaData} repository. Internal implementation class, use
 * {@link MetaDataServiceImpl} instead.
 */
class AttributeMetaDataRepository
{
	private static final int BATCH_SIZE = 1000;

	public static final AttributeMetaDataMetaData META_DATA = AttributeMetaDataMetaData.INSTANCE;

	private final UuidGenerator uuidGenerator;
	private final Repository repository;
	private EntityMetaDataRepository entityMetaDataRepository;
	private final LanguageService languageService;

	public AttributeMetaDataRepository(ManageableRepositoryCollection collection, LanguageService languageService)
	{
		this.repository = requireNonNull(collection).addEntityMeta(META_DATA);
		uuidGenerator = new UuidGenerator();
		this.languageService = languageService;
	}

	public void setEntityMetaDataRepository(EntityMetaDataRepository entityMetaDataRepository)
	{
		this.entityMetaDataRepository = entityMetaDataRepository;
	}

	Repository getRepository()
	{
		return repository;
	}

	/**
	 * Adds an attribute to the repository and returns the Entity it's created for it. If the attribute is a compound
	 * attribute with attribute parts, will also add all the parts.
	 * 
	 * @param att
	 *            AttributeMetaData to add
	 * @return the AttributeMetaDataMetaData entity that got created
	 */
	public Entity add(AttributeMetaData att)
	{
		return add(Arrays.asList(att)).iterator().next();
	}

	public Iterable<Entity> add(Iterable<AttributeMetaData> attrs)
	{
		Iterable<List<AttributeMetaData>> batches = Iterables.partition(attrs, BATCH_SIZE);
		return new Iterable<Entity>()
		{
			@Override
			public Iterator<Entity> iterator()
			{
				return stream(batches.spliterator(), false).flatMap(batch -> {
					List<Entity> attrEntities = convertToAttrEntities(attrs);
					repository.add(attrEntities.stream());
					return attrEntities.stream();
				}).iterator();
			}

			private List<Entity> convertToAttrEntities(Iterable<AttributeMetaData> attrs)
			{
				return stream(attrs.spliterator(), false).map(this::convertToAttrEntity).collect(toList());
			}

			private Entity convertToAttrEntity(AttributeMetaData attr)
			{
				Entity attrEntity = MetaUtils.toEntity(attr);
				attrEntity.set(IDENTIFIER, uuidGenerator.generateId());
				return attrEntity;
			}
		};
	}

	/**
	 * Deletes attributes from the repository. If the attribute is a compound attribute with attribute parts, also
	 * deletes its parts.
	 * 
	 * @param attributes
	 *            Iterable<Entity> for the attribute that should be deleted
	 */
	public void deleteAttributes(Iterable<Entity> attributes)
	{
		if (attributes != null)
		{
			for (Entity attribute : attributes)
			{
				deleteAttributes(attribute.getEntities(PARTS));
				repository.delete(attribute);
			}
		}
	}

	/**
	 * Deletes all Attributes from the repository.
	 */
	public void deleteAll()
	{
		repository.deleteAll();
	}

	/**
	 * Creates a {@link DefaultAttributeMetaData} instance for an Entity in the repository.
	 * 
	 * @param entity
	 *            {@link AttributeMetaDataMetaData} Entity
	 * @return {@link DefaultAttributeMetaData}, with {@link DefaultAttributeMetaData#getRefEntity()} properly filled if
	 *         needed.
	 */
	public AttributeMetaData toAttributeMetaData(Entity entity)
	{
		return MetaUtils.toAttribute(entity, entityMetaDataRepository.getRepository(),
				languageService.getLanguageCodes());
	}
}