package org.molgenis.data.meta;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.AGGREGATEABLE;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.AUTO;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.DATA_TYPE;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.DEFAULT_VALUE;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.DESCRIPTION;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.ENUM_OPTIONS;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.EXPRESSION;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.IDENTIFIER;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.LABEL;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.NAME;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.NILLABLE;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.PARTS;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.RANGE_MAX;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.RANGE_MIN;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.READ_ONLY;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.REF_ENTITY;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.UNIQUE;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.VALIDATION_EXPRESSION;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.VISIBLE;
import static org.molgenis.data.meta.AttributeMetaDataMetaData.VISIBLE_EXPRESSION;
import static org.molgenis.util.SecurityDecoratorUtils.validatePermission;

import java.util.Arrays;
import java.util.List;

import org.molgenis.MolgenisFieldTypes;
import org.molgenis.MolgenisFieldTypes.FieldTypeEnum;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.MolgenisDataAccessException;
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.Package;
import org.molgenis.data.Range;
import org.molgenis.data.Repository;
import org.molgenis.data.semantic.LabeledResource;
import org.molgenis.data.semantic.Relation;
import org.molgenis.data.semantic.Tag;
import org.molgenis.data.semantic.TagImpl;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.molgenis.data.support.MapEntity;
import org.molgenis.fieldtypes.FieldType;
import org.molgenis.security.core.Permission;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class MetaUtils
{
	/**
	 * Converts a tag to an entity
	 * 
	 * @param tag
	 * @return
	 */
	public static Entity toEntity(Tag<?, LabeledResource, LabeledResource> tag)
	{
		MapEntity tagEntity = new MapEntity(TagMetaData.INSTANCE);
		tagEntity.set(TagMetaData.IDENTIFIER, tag.getIdentifier());
		tagEntity.set(TagMetaData.OBJECT_IRI, tag.getObject().getIri());
		tagEntity.set(TagMetaData.LABEL, tag.getObject().getLabel());
		tagEntity.set(TagMetaData.RELATION_IRI, tag.getRelation().getIRI());
		tagEntity.set(TagMetaData.RELATION_LABEL, tag.getRelation().getLabel());
		tagEntity.set(TagMetaData.CODE_SYSTEM, tag.getCodeSystem().getIri());
		return tagEntity;
	}

	/**
	 * Converts an attribute to an entity
	 * 
	 * @param attr
	 * @return
	 */
	public static Entity toEntity(AttributeMetaData attr)
	{
		MapEntity entity = new MapEntity(AttributeMetaDataMetaData.INSTANCE);
		entity.set(AttributeMetaDataMetaData.IDENTIFIER, attr.getIdentifier());
		entity.set(AttributeMetaDataMetaData.NAME, attr.getName());
		FieldType dataType = attr.getDataType();
		if (dataType != null)
		{
			entity.set(AttributeMetaDataMetaData.DATA_TYPE,
					dataType.getEnumType().toString().toLowerCase().replace("_", "")); // TODO move logic somewhere else
		}
		EntityMetaData refEntity = attr.getRefEntity();
		if (refEntity != null)
		{
			entity.set(AttributeMetaDataMetaData.REF_ENTITY, refEntity.getName());
		}
		entity.set(AttributeMetaDataMetaData.EXPRESSION, attr.getExpression());
		entity.set(AttributeMetaDataMetaData.NILLABLE, attr.isNillable());
		entity.set(AttributeMetaDataMetaData.AUTO, attr.isAuto());
		entity.set(AttributeMetaDataMetaData.VISIBLE, attr.isVisible());
		entity.set(AttributeMetaDataMetaData.LABEL, attr.getLabel());
		attr.getLabelLanguageCodes().forEach(languageCode -> {
			entity.set(EntityMetaDataMetaData.LABEL + '-' + languageCode, attr.getLabel(languageCode));
		});
		entity.set(AttributeMetaDataMetaData.DESCRIPTION, attr.getDescription());
		attr.getDescriptionLanguageCodes().forEach(languageCode -> {
			entity.set(EntityMetaDataMetaData.DESCRIPTION + '-' + languageCode, attr.getDescription(languageCode));
		});
		entity.set(AttributeMetaDataMetaData.AGGREGATEABLE, attr.isAggregateable());
		List<String> enumOptions = attr.getEnumOptions();
		if (enumOptions != null && !enumOptions.isEmpty())
		{
			entity.set(AttributeMetaDataMetaData.ENUM_OPTIONS, enumOptions.stream().collect(joining(",")));
		}
		Range range = attr.getRange();
		if (range != null)
		{
			Long rangeMin = range.getMin();
			if (rangeMin != null)
			{
				entity.set(AttributeMetaDataMetaData.RANGE_MIN, rangeMin);
			}
			Long rangeMax = range.getMax();
			if (rangeMax != null)
			{
				entity.set(AttributeMetaDataMetaData.RANGE_MAX, rangeMax);
			}
		}
		entity.set(AttributeMetaDataMetaData.READ_ONLY, attr.isReadonly());
		entity.set(AttributeMetaDataMetaData.UNIQUE, attr.isUnique());
		Iterable<AttributeMetaData> attrParts = attr.getAttributeParts();
		if (attrParts != null)
		{
			entity.set(AttributeMetaDataMetaData.PARTS,
					stream(attrParts.spliterator(), false).map(MetaUtils::toEntity).collect(toList()));
		}
		entity.set(AttributeMetaDataMetaData.VISIBLE_EXPRESSION, attr.getVisibleExpression());
		entity.set(AttributeMetaDataMetaData.VALIDATION_EXPRESSION, attr.getValidationExpression());
		entity.set(AttributeMetaDataMetaData.DEFAULT_VALUE, attr.getDefaultValue());
		entity.set(AttributeMetaDataMetaData.TAGS,
				stream(attr.getTags().spliterator(), false).map(MetaUtils::toEntity).collect(toList()));
		return entity;
	}

	/**
	 * Converts an entity meta data to an entity
	 * 
	 * @param entityMeta
	 * @return
	 */
	public static Entity toEntity(EntityMetaData entityMeta)
	{
		MapEntity entity = new MapEntity(EntityMetaDataMetaData.INSTANCE);
		entity.set(EntityMetaDataMetaData.SIMPLE_NAME, entityMeta.getSimpleName());
		entity.set(EntityMetaDataMetaData.BACKEND, entityMeta.getBackend());
		entity.set(EntityMetaDataMetaData.FULL_NAME, entityMeta.getName());
		AttributeMetaData idAttr = entityMeta.getOwnIdAttribute();
		if (idAttr != null)
		{
			entity.set(EntityMetaDataMetaData.ID_ATTRIBUTE, toEntity(idAttr));
		}
		AttributeMetaData labelAttr = entityMeta.getOwnLabelAttribute();
		if (labelAttr != null)
		{
			entity.set(EntityMetaDataMetaData.LABEL_ATTRIBUTE, toEntity(labelAttr));
		}
		Iterable<AttributeMetaData> lookupAttrs = entityMeta.getOwnLookupAttributes();
		if (lookupAttrs != null)
		{
			entity.set(EntityMetaDataMetaData.LOOKUP_ATTRIBUTES,
					stream(lookupAttrs.spliterator(), false).map(MetaUtils::toEntity).collect(toList()));
		}
		entity.set(EntityMetaDataMetaData.ABSTRACT, entityMeta.isAbstract());
		entity.set(EntityMetaDataMetaData.SYSTEM, entityMeta.isSystem());
		entity.set(EntityMetaDataMetaData.LABEL, entityMeta.getLabel());
		entityMeta.getLabelLanguageCodes().forEach(languageCode -> {
			entity.set(EntityMetaDataMetaData.LABEL + '-' + languageCode, entityMeta.getLabel(languageCode));
		});
		EntityMetaData extendsEntityMeta = entityMeta.getExtends();
		if (extendsEntityMeta != null)
		{
			entity.set(EntityMetaDataMetaData.EXTENDS, toEntity(extendsEntityMeta));
		}
		entity.set(EntityMetaDataMetaData.DESCRIPTION, entityMeta.getDescription());
		entityMeta.getDescriptionLanguageCodes().forEach(languageCode -> {
			entity.set(EntityMetaDataMetaData.DESCRIPTION + '-' + languageCode,
					entityMeta.getDescription(languageCode));
		});
		Package package_ = entityMeta.getPackage();
		if (package_ != null)
		{
			entity.set(EntityMetaDataMetaData.PACKAGE, toEntity(package_));
		}

		entity.set(EntityMetaDataMetaData.TAGS,
				stream(entityMeta.getOwnTags().spliterator(), false).map(MetaUtils::toEntity).collect(toList()));
		entity.set(EntityMetaDataMetaData.ATTRIBUTES,
				stream(entityMeta.getOwnAttributes().spliterator(), false).map(MetaUtils::toEntity).collect(toList()));
		return entity;
	}

	/**
	 * Converts a package to an entity
	 * 
	 * @param package_
	 * @return
	 */
	public static Entity toEntity(Package package_)
	{
		MapEntity entity = new MapEntity(PackageMetaData.INSTANCE);
		entity.set(PackageMetaData.FULL_NAME, package_.getName());
		entity.set(PackageMetaData.SIMPLE_NAME, package_.getSimpleName());
		entity.set(PackageMetaData.DESCRIPTION, package_.getDescription());
		Package parentPackage = package_.getParent();
		if (parentPackage != null)
		{
			entity.set(PackageMetaData.PARENT, toEntity(parentPackage));
		}
		entity.set(PackageMetaData.TAGS,
				stream(package_.getTags().spliterator(), false).map(MetaUtils::toEntity).collect(toList()));
		return entity;
	}

	/**
	 * Converts an entity to a tag
	 * 
	 * @param tagEntity
	 * @param subjectType
	 * @return
	 */
	public static <SubjectType> Tag<SubjectType, LabeledResource, LabeledResource> toTag(Entity tagEntity,
			SubjectType subjectType)
	{
		String identifier = tagEntity.getString(TagMetaData.IDENTIFIER);
		String relationIri = tagEntity.getString(TagMetaData.RELATION_IRI);
		Relation relation = Relation.forIRI(relationIri);
		if (relation == null)
		{
			throw new IllegalArgumentException("Unknown relation iri [" + relationIri + "]");
		}

		LabeledResource codeSystem = null;
		if (tagEntity.getString(TagMetaData.CODE_SYSTEM) != null)
		{
			codeSystem = new LabeledResource(tagEntity.getString(TagMetaData.CODE_SYSTEM),
					tagEntity.getString(TagMetaData.CODE_SYSTEM));
		}

		LabeledResource objectResource = new LabeledResource(tagEntity.getString(TagMetaData.OBJECT_IRI),
				tagEntity.getString(TagMetaData.LABEL));

		return new TagImpl<SubjectType, LabeledResource, LabeledResource>(identifier, subjectType, relation,
				objectResource, codeSystem);
	}

	/**
	 * Converts a entity to an attribute
	 * 
	 * @param attrEntity
	 * @param entityMetaRepo
	 * @param languageCodes
	 * @return
	 */
	public static AttributeMetaData toAttribute(Entity attrEntity, Repository entityMetaRepo,
			List<String> languageCodes)
	{
		DefaultAttributeMetaData attributeMetaData = new DefaultAttributeMetaData(attrEntity.getString(NAME));
		attributeMetaData.setIdentifier(attrEntity.getString(IDENTIFIER));
		attributeMetaData.setDataType(MolgenisFieldTypes.getType(attrEntity.getString(DATA_TYPE)));
		Boolean nillable = attrEntity.getBoolean(NILLABLE);
		attributeMetaData.setNillable(nillable != null ? nillable.booleanValue() : false);
		Boolean auto = attrEntity.getBoolean(AUTO);
		attributeMetaData.setAuto(auto != null ? auto.booleanValue() : false);
		Boolean visible = attrEntity.getBoolean(VISIBLE);
		attributeMetaData.setVisible(visible != null ? visible.booleanValue() : false);
		attributeMetaData.setLabel(attrEntity.getString(LABEL));
		attributeMetaData.setDescription(attrEntity.getString(DESCRIPTION));
		Boolean aggregateable = attrEntity.getBoolean(AGGREGATEABLE);
		attributeMetaData.setAggregateable(aggregateable != null ? aggregateable.booleanValue() : false);
		attributeMetaData.setEnumOptions(attrEntity.getList(ENUM_OPTIONS));
		Boolean readonly = attrEntity.getBoolean(READ_ONLY);
		attributeMetaData.setReadOnly(readonly != null ? readonly.booleanValue() : false);
		Boolean unique = attrEntity.getBoolean(UNIQUE);
		attributeMetaData.setUnique(unique != null ? unique.booleanValue() : false);
		attributeMetaData.setExpression(attrEntity.getString(EXPRESSION));

		Long rangeMin = attrEntity.getLong(RANGE_MIN);
		Long rangeMax = attrEntity.getLong(RANGE_MAX);
		if ((rangeMin != null) || (rangeMax != null))
		{
			attributeMetaData.setRange(new Range(rangeMin, rangeMax));
		}
		if (attrEntity.get(REF_ENTITY) != null)
		{
			final String refEntityName = attrEntity.getString(REF_ENTITY);
			EntityMetaData refEntity = toEntityMeta(entityMetaRepo.findOne(refEntityName), entityMetaRepo,
					languageCodes);
			attributeMetaData.setRefEntity(refEntity);
		}
		Iterable<Entity> parts = attrEntity.getEntities(PARTS);
		if (parts != null)
		{
			stream(parts.spliterator(), false)
					.map(attrPart -> MetaUtils.toAttribute(attrPart, entityMetaRepo, languageCodes))
					.forEach(attributeMetaData::addAttributePart);
		}
		attributeMetaData.setVisibleExpression(attrEntity.getString(VISIBLE_EXPRESSION));
		attributeMetaData.setValidationExpression(attrEntity.getString(VALIDATION_EXPRESSION));
		attributeMetaData.setDefaultValue(attrEntity.getString(DEFAULT_VALUE));

		// Language attributes
		for (String languageCode : languageCodes)
		{
			String attributeName = LABEL + '-' + languageCode;
			String label = attrEntity.getString(attributeName);
			if (label != null) attributeMetaData.setLabel(languageCode, label);

			attributeName = DESCRIPTION + '-' + languageCode;
			String description = attrEntity.getString(attributeName);
			if (description != null) attributeMetaData.setDescription(languageCode, description);
		}

		return attributeMetaData;
	}

	/**
	 * Converts an entity to a entity meta data
	 * 
	 * @param entityEntity
	 * @param entityMetaRepo
	 * @param languageCodes
	 * @return
	 */
	public static EntityMetaData toEntityMeta(Entity entityEntity, Repository entityMetaRepo,
			List<String> languageCodes)
	{
		String simpleName = entityEntity.getString(EntityMetaDataMetaData.SIMPLE_NAME);
		DefaultEntityMetaData entityMeta = new DefaultEntityMetaData(simpleName);
		entityMeta.setBackend(entityEntity.getString(EntityMetaDataMetaData.BACKEND));
		Entity idAttrEntity = entityEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE);
		if (idAttrEntity != null)
		{
			entityMeta.setIdAttribute(toAttribute(idAttrEntity, entityMetaRepo, languageCodes));
		}
		Entity labelAttrEntity = entityEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE);
		if (labelAttrEntity != null)
		{
			entityMeta.setLabelAttribute(toAttribute(labelAttrEntity, entityMetaRepo, languageCodes));
		}
		Iterable<Entity> lookupAttrEntities = entityEntity.getEntities(EntityMetaDataMetaData.LOOKUP_ATTRIBUTES);
		entityMeta.setLookupAttributes(stream(lookupAttrEntities.spliterator(), false)
				.map(lookupAttrEntity -> toAttribute(lookupAttrEntity, entityMetaRepo, languageCodes)));
		entityMeta.setAbstract(entityEntity.getBoolean(EntityMetaDataMetaData.ABSTRACT));
		entityMeta.setLabel(entityEntity.getString(EntityMetaDataMetaData.LABEL));
		Entity extendsEntity = entityEntity.getEntity(EntityMetaDataMetaData.EXTENDS);
		if (extendsEntity != null)
		{
			entityMeta.setExtends(toEntityMeta(extendsEntity, entityMetaRepo, languageCodes));
		}
		entityMeta.setDescription(entityEntity.getString(EntityMetaDataMetaData.DESCRIPTION));
		Entity packageEntity = entityEntity.getEntity(EntityMetaDataMetaData.PACKAGE);
		if (packageEntity != null)
		{
			entityMeta.setPackage(toPackage(packageEntity));
		}

		Iterable<Entity> attrEntities = entityEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES);
		stream(attrEntities.spliterator(), false)
				.map(lookupAttrEntity -> toAttribute(lookupAttrEntity, entityMetaRepo, languageCodes)).forEach(attr -> {
					entityMeta.addAttributeMetaData(attr);
				});
		entityMeta.setSystem(entityEntity.getBoolean(EntityMetaDataMetaData.SYSTEM));
		return entityMeta;
	}

	/**
	 * Converts an entity to a package
	 * 
	 * @param packageEntity
	 * @return
	 */
	public static Package toPackage(Entity packageEntity)
	{
		String simpleName = packageEntity.getString(PackageMetaData.SIMPLE_NAME);
		String description = packageEntity.getString(PackageMetaData.DESCRIPTION);
		Entity parentPackageEntity = packageEntity.getEntity(PackageMetaData.PARENT);
		PackageImpl package_;
		if (parentPackageEntity != null)
		{
			package_ = new PackageImpl(simpleName, description, toPackage(parentPackageEntity));
		}
		else
		{
			package_ = new PackageImpl(simpleName, description);
		}
		Iterable<Entity> tagEntities = packageEntity.getEntities(PackageMetaData.TAGS);
		tagEntities.forEach(tagEntity -> {
			package_.addTag(MetaUtils.toTag(tagEntity, package_));
		});
		return package_;
	}

	public static List<AttributeMetaData> updateEntityMeta(MetaDataService metaDataService, EntityMetaData entityMeta,
			boolean sync)
	{
		String backend = entityMeta.getBackend() != null ? entityMeta.getBackend()
				: metaDataService.getDefaultBackend().getName();

		EntityMetaData existingEntityMetaData = metaDataService.getEntityMetaData(entityMeta.getName());
		if (!existingEntityMetaData.getBackend().equals(backend))
		{
			throw new MolgenisDataException(
					"Changing the backend of an entity is not supported. You tried to change the backend of entity '"
							+ entityMeta.getName() + "' from '" + existingEntityMetaData.getBackend() + "' to '"
							+ backend + "'");
		}

		List<AttributeMetaData> addedAttributes = Lists.newArrayList();

		for (AttributeMetaData attr : existingEntityMetaData.getAttributes())
		{
			if (entityMeta.getAttribute(attr.getName()) == null)
			{
				throw new MolgenisDataException(
						"Removing of existing attributes is currently not supported. You tried to remove attribute ["
								+ attr.getName() + "] of entity [" + entityMeta.getName() + "]");
			}
		}

		for (AttributeMetaData attr : entityMeta.getAttributes())
		{
			AttributeMetaData currentAttribute = existingEntityMetaData.getAttribute(attr.getName());
			if (currentAttribute != null)
			{
				if (!currentAttribute.isSameAs(attr))
				{
					throw new MolgenisDataException(
							"Changing existing attributes is not currently supported. You tried to alter attribute ["
									+ attr.getName() + "] of entity [" + entityMeta.getName()
									+ "]. Only adding of new attributes is supported.");
				}
			}
			else if (!attr.isNillable())
			{
				throw new MolgenisDataException(
						"Adding non-nillable attributes is not currently supported.  You tried to add non-nillable attribute ["
								+ attr.getName() + "] of entity [" + entityMeta.getName() + "].");
			}
			else
			{
				validatePermission(entityMeta.getName(), Permission.WRITEMETA);

				if (sync) metaDataService.addAttributeSync(entityMeta.getName(), attr);
				else metaDataService.addAttribute(entityMeta.getName(), attr);

				addedAttributes.add(attr);
			}
		}

		return addedAttributes;
	}

	/**
	 * Convert a list of AttributeMetaDataEntity to AttributeMetaData
	 * 
	 * @param entityMetaData
	 * @param attributeMetaDataEntities
	 * @return
	 */
	public static Iterable<AttributeMetaData> toExistingAttributeMetaData(EntityMetaData entityMetaData,
			Iterable<Entity> attributeMetaDataEntities)
	{
		return FluentIterable.from(attributeMetaDataEntities).transform(new Function<Entity, AttributeMetaData>()
		{
			@Override
			public AttributeMetaData apply(Entity attributeMetaDataEntity)
			{
				String attributeName = attributeMetaDataEntity.getString(AttributeMetaDataMetaData.NAME);
				AttributeMetaData attribute = entityMetaData.getAttribute(attributeName);
				if (attribute == null) throw new MolgenisDataAccessException("The attributeMetaData : " + attributeName
						+ " does not exsit in EntityMetaData : " + entityMetaData.getName());
				return attribute;
			}
		}).toList();
	}

	/**
	 * Compares a Entity and EntityMetaData representation of entity meta data. For references to other entities (e.g.
	 * representing attributes and packages) checks if both representations refer to the same identifiers.
	 * 
	 * Tags as well as i18n label and 18n description attributes are not considered in the comparison.
	 * 
	 * @param entityMetaEntity
	 * @param entityMeta
	 * @param metaDataService
	 * @return true if entityMetaEntity and entityMeta represent the same meta data
	 */
	public static boolean equals(Entity entityMetaEntity, EntityMetaData entityMeta, MetaDataService metaDataService)
	{
		if (!Objects.equal(entityMetaEntity.getString(EntityMetaDataMetaData.SIMPLE_NAME), entityMeta.getSimpleName()))
		{
			return false;
		}

		String backend = entityMeta.getBackend();
		if (backend == null)
		{
			// use default backend if entity stored in repository has backend and entity meta has no backend
			backend = metaDataService.getDefaultBackend().getName();
		}
		if (!Objects.equal(entityMetaEntity.getString(EntityMetaDataMetaData.BACKEND), backend))
		{
			return false;
		}

		if (!Objects.equal(entityMetaEntity.getString(EntityMetaDataMetaData.FULL_NAME), entityMeta.getName()))
		{
			return false;
		}

		Entity idAttrEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE);
		String thisIdAttrName = idAttrEntity != null ? idAttrEntity.getString(AttributeMetaDataMetaData.NAME) : null;
		AttributeMetaData idAttr = entityMeta.getOwnIdAttribute();
		String otherIdAttrName = idAttr != null ? idAttr.getName() : null;
		if (!Objects.equal(thisIdAttrName, otherIdAttrName))
		{
			return false;
		}

		Entity lblAttrEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE);
		String thisLblAttrName = lblAttrEntity != null ? lblAttrEntity.getString(AttributeMetaDataMetaData.NAME) : null;
		AttributeMetaData lblAttr = entityMeta.getOwnLabelAttribute();
		String otherLblAttrName = lblAttr != null ? lblAttr.getName() : null;
		if (!Objects.equal(thisLblAttrName, otherLblAttrName))
		{
			return false;
		}

		List<String> thisLookupAttrNames = stream(
				entityMetaEntity.getEntities(EntityMetaDataMetaData.LOOKUP_ATTRIBUTES).spliterator(), false)
						.map(entity -> entity.getString(AttributeMetaDataMetaData.NAME)).collect(toList());
		List<String> otherLookupAttrNames = stream(entityMeta.getOwnLookupAttributes().spliterator(), false)
				.map(AttributeMetaData::getName).collect(toList());
		if (!Objects.equal(thisLookupAttrNames, otherLookupAttrNames))
		{
			return false;
		}

		if (!Objects.equal(entityMetaEntity.getBoolean(EntityMetaDataMetaData.ABSTRACT), entityMeta.isAbstract()))
		{
			return false;
		}

		if (!Objects.equal(entityMetaEntity.getString(EntityMetaDataMetaData.LABEL), entityMeta.getLabel()))
		{
			return false;
		}

		Entity extendsEntityEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.EXTENDS);
		String thisExtendsEntityName = extendsEntityEntity != null
				? extendsEntityEntity.getString(EntityMetaDataMetaData.FULL_NAME) : null;
		EntityMetaData extendsEntity = entityMeta.getExtends();
		String otherExtendsEntityName = extendsEntity != null ? extendsEntity.getName() : null;
		if (!Objects.equal(thisExtendsEntityName, otherExtendsEntityName))
		{
			return false;
		}

		if (!Objects.equal(entityMetaEntity.getString(EntityMetaDataMetaData.DESCRIPTION), entityMeta.getDescription()))
		{
			return false;
		}

		Entity packageEntity = entityMetaEntity.getEntity(EntityMetaDataMetaData.PACKAGE);
		String thisPackageName = packageEntity != null ? packageEntity.getString(PackageMetaData.FULL_NAME) : null;
		Package package_ = entityMeta.getPackage();
		if (package_ == null)
		{
			// use default package if entity stored in repository has package and entity meta has no package
			package_ = PackageImpl.defaultPackage;
		}
		String otherPackageName = package_ != null ? package_.getName() : null;
		if (!Objects.equal(thisPackageName, otherPackageName))
		{
			return false;
		}

		List<String> thisAttrNames = stream(
				entityMetaEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES).spliterator(), false)
						.map(entity -> entity.getString(AttributeMetaDataMetaData.NAME)).collect(toList());
		List<String> otherAttrNames = stream(entityMeta.getOwnAttributes().spliterator(), false)
				.map(AttributeMetaData::getName).collect(toList());
		if (!Objects.equal(thisAttrNames, otherAttrNames))
		{
			return false;
		}

		return true;
	}

	/**
	 * Compares a Entity and AttributeMetaData representation of attribute meta data. For references to other entities
	 * (e.g. representing entities and packages) checks if both representations refer to the same identifiers.
	 * 
	 * Tags as well as i18n label and 18n description attributes are not considered in the comparison.
	 * 
	 * @param attrEntity
	 * @param attr
	 * @return true if attrEntity and attr represent the same meta data
	 */
	public static boolean equals(Entity attrEntity, AttributeMetaData attr)
	{
		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.NAME), attr.getName()))
		{
			return false;
		}

		String dataType = attrEntity.getString(AttributeMetaDataMetaData.DATA_TYPE);
		String otherDataType = attr.getDataType().getEnumType().toString().replace("_", "").toLowerCase();
		if (!Objects.equal(dataType, otherDataType))
		{
			return false;
		}

		String thisRefEntityName = attrEntity.getString(AttributeMetaDataMetaData.REF_ENTITY);
		EntityMetaData otherRefEntity = attr.getRefEntity();
		String otherRefEntityName = otherRefEntity != null ? otherRefEntity.getName() : null;
		if (!Objects.equal(thisRefEntityName, otherRefEntityName))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.EXPRESSION), attr.getExpression()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getBoolean(AttributeMetaDataMetaData.NILLABLE), attr.isNillable()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getBoolean(AttributeMetaDataMetaData.AUTO), attr.isAuto()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getBoolean(AttributeMetaDataMetaData.VISIBLE), attr.isVisible()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.LABEL), attr.getLabel()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.DESCRIPTION), attr.getDescription()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getBoolean(AttributeMetaDataMetaData.AGGREGATEABLE), attr.isAggregateable()))
		{
			return false;
		}

		String thisEnumOptionsStr = attrEntity.getString(AttributeMetaDataMetaData.ENUM_OPTIONS);
		List<String> thisEnumOptions = thisEnumOptionsStr != null ? Arrays.asList(thisEnumOptionsStr.split(",")) : null;
		List<String> otherEnumOptions = attr.getEnumOptions();
		if (!Objects.equal(thisEnumOptions, otherEnumOptions))
		{
			return false;
		}

		Long thisRangeMin = attrEntity.getLong(AttributeMetaDataMetaData.RANGE_MIN);
		Long otherRangeMin = attr.getRange() != null ? attr.getRange().getMin() : null;
		if (!Objects.equal(thisRangeMin, otherRangeMin))
		{
			return false;
		}

		Long thisRangeMax = attrEntity.getLong(AttributeMetaDataMetaData.RANGE_MAX);
		Long otherRangeMax = attr.getRange() != null ? attr.getRange().getMax() : null;
		if (!Objects.equal(thisRangeMax, otherRangeMax))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getBoolean(AttributeMetaDataMetaData.READ_ONLY), attr.isReadonly()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getBoolean(AttributeMetaDataMetaData.UNIQUE), attr.isUnique()))
		{
			return false;
		}

		Iterable<Entity> thisAttrParts = attrEntity.getEntities(AttributeMetaDataMetaData.PARTS);
		List<String> thisAttrPartNames = stream(thisAttrParts.spliterator(), false)
				.map(attrPart -> attrPart.getString(AttributeMetaDataMetaData.NAME)).collect(toList());
		Iterable<AttributeMetaData> otherAttrParts = attr.getAttributeParts();
		List<String> otherAttrPartNames = stream(otherAttrParts.spliterator(), false).map(AttributeMetaData::getName)
				.collect(toList());
		if (!Objects.equal(thisAttrPartNames, otherAttrPartNames))
		{
			return false;
		}

		for (Entity thisAttrPart : thisAttrParts)
		{
			String attrPartName = thisAttrPart.getString(AttributeMetaDataMetaData.NAME);
			String attrPartDataType = thisAttrPart.getString(AttributeMetaDataMetaData.DATA_TYPE);
			if (attrPartDataType.equals(FieldTypeEnum.COMPOUND.toString().toLowerCase()))
			{
				if (!equals(thisAttrPart, attr.getAttributePart(attrPartName)))
				{
					return false;
				}
			}
		}

		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.VISIBLE_EXPRESSION),
				attr.getVisibleExpression()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.VALIDATION_EXPRESSION),
				attr.getValidationExpression()))
		{
			return false;
		}

		if (!Objects.equal(attrEntity.getString(AttributeMetaDataMetaData.DEFAULT_VALUE), attr.getDefaultValue()))
		{
			return false;
		}

		return true;
	}
}
