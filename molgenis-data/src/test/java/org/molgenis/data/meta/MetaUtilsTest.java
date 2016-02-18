package org.molgenis.data.meta;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.molgenis.MolgenisFieldTypes.COMPOUND;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;

import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.Entity;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.MolgenisDataAccessException;
import org.molgenis.data.Package;
import org.molgenis.data.Range;
import org.molgenis.data.semantic.LabeledResource;
import org.molgenis.data.semantic.Relation;
import org.molgenis.data.semantic.Tag;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.molgenis.data.support.MapEntity;
import org.molgenis.fieldtypes.FieldType;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MetaUtilsTest
{
	@SuppressWarnings("unchecked")
	@Test
	public void toEntityTag()
	{
		String tagIdentifier = "id0";
		Relation tagRelation = Relation.instanceOf;
		String tagObjectIri = "objectIri";
		String tagObjectLabel = "objectLabel";
		String codeSystemIri = "codeSystemIri";
		String codeSystemLabel = "codeSystemLabel";

		Tag<String, LabeledResource, LabeledResource> tag = mock(Tag.class);
		when(tag.getRelation()).thenReturn(tagRelation);
		when(tag.getSubject()).thenReturn("subject");
		when(tag.getObject()).thenReturn(new LabeledResource(tagObjectIri, tagObjectLabel));
		when(tag.getCodeSystem()).thenReturn(new LabeledResource(codeSystemIri, codeSystemLabel));
		when(tag.getIdentifier()).thenReturn(tagIdentifier);

		Entity tagEntity = MetaUtils.toEntity(tag);
		assertEquals(tagEntity.getString(TagMetaData.IDENTIFIER), tagIdentifier);
		assertEquals(tagEntity.getString(TagMetaData.OBJECT_IRI), tagObjectIri);
		assertEquals(tagEntity.getString(TagMetaData.LABEL), tagObjectLabel);
		assertEquals(tagEntity.getString(TagMetaData.RELATION_IRI), tagRelation.getIRI());
		assertEquals(tagEntity.getString(TagMetaData.RELATION_LABEL), tagRelation.getLabel());
		assertEquals(tagEntity.getString(TagMetaData.CODE_SYSTEM), codeSystemIri);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void toEntityAttributeMetaData()
	{
		String attrIdentifier = "id0";
		String attrName = "attr0";
		FieldType attrDataType = COMPOUND;
		boolean attrNillable = false;
		boolean attrAuto = false;
		boolean attrVisible = true;
		String attrLabel = "label0";
		String attrLabelNl = "labelNl0";
		String attrLabelEn = "labelEn0";
		String attrDescription = "description0";
		String attrDescriptionDe = "descriptionDe0";
		String attrDescriptionFr = "descriptionFr0";
		boolean attrAggregateable = true;
		boolean attrReadonly = false;
		boolean attrUnique = true;
		String attrExpression = "expression0";
		String attrVisibleExpression = "visibleExpression0";
		String attrValidationExpression = "validationExpression0";
		String attrDefaultValue = "attrDefaultValue0";

		AttributeMetaData attrPart0 = mock(AttributeMetaData.class);
		when(attrPart0.getAttributeParts()).thenReturn(emptyList());
		when(attrPart0.getTags()).thenReturn(emptyList());
		AttributeMetaData attrPart1 = mock(AttributeMetaData.class);
		when(attrPart1.getTags()).thenReturn(emptyList());
		when(attrPart1.getAttributeParts()).thenReturn(emptyList());

		String tagIdentifier = "id0";
		Relation tagRelation = Relation.instanceOf;
		String tagObjectIri = "objectIri";
		String tagObjectLabel = "objectLabel";
		String codeSystemIri = "codeSystemIri";
		String codeSystemLabel = "codeSystemLabel";

		String refEntityName = "refEntity0";
		EntityMetaData refEntityMeta = mock(EntityMetaData.class);
		when(refEntityMeta.getName()).thenReturn(refEntityName);

		AttributeMetaData attr = mock(AttributeMetaData.class);

		Tag<AttributeMetaData, LabeledResource, LabeledResource> tag0 = mock(Tag.class);
		when(tag0.getRelation()).thenReturn(tagRelation);
		when(tag0.getSubject()).thenReturn(attr);
		when(tag0.getObject()).thenReturn(new LabeledResource(tagObjectIri, tagObjectLabel));
		when(tag0.getCodeSystem()).thenReturn(new LabeledResource(codeSystemIri, codeSystemLabel));
		when(tag0.getIdentifier()).thenReturn(tagIdentifier);

		when(attr.getIdentifier()).thenReturn(attrIdentifier);
		when(attr.getName()).thenReturn(attrName);
		when(attr.getDataType()).thenReturn(attrDataType);
		when(attr.isNillable()).thenReturn(attrNillable);
		when(attr.isAuto()).thenReturn(attrAuto);
		when(attr.isVisible()).thenReturn(attrVisible);
		when(attr.getLabel()).thenReturn(attrLabel);
		when(attr.getDescription()).thenReturn(attrDescription);
		when(attr.isAggregateable()).thenReturn(attrAggregateable);
		when(attr.isReadonly()).thenReturn(attrReadonly);
		when(attr.isUnique()).thenReturn(attrUnique);
		when(attr.getAttributeParts()).thenReturn(Arrays.asList(attrPart0, attrPart1));
		when(attr.getTags()).thenReturn(singletonList(tag0));
		when(attr.getVisibleExpression()).thenReturn(attrVisibleExpression);
		when(attr.getValidationExpression()).thenReturn(attrValidationExpression);
		when(attr.getRefEntity()).thenReturn(refEntityMeta);
		when(attr.getExpression()).thenReturn(attrExpression);
		when(attr.getEnumOptions()).thenReturn(Arrays.asList("a", "b", "c"));
		Range range = new Range(1l, 3l);
		when(attr.getRange()).thenReturn(range);
		when(attr.getDefaultValue()).thenReturn(attrDefaultValue);
		when(attr.getLabelLanguageCodes()).thenReturn(Sets.newHashSet("nl", "en"));
		when(attr.getLabel("nl")).thenReturn(attrLabelNl);
		when(attr.getLabel("en")).thenReturn(attrLabelEn);
		when(attr.getDescriptionLanguageCodes()).thenReturn(Sets.newHashSet("de", "fr"));
		when(attr.getDescription("de")).thenReturn(attrDescriptionDe);
		when(attr.getDescription("fr")).thenReturn(attrDescriptionFr);

		Entity attrEntity = MetaUtils.toEntity(attr);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.IDENTIFIER), attrIdentifier);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.NAME), attrName);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.DATA_TYPE), attrDataType.toString().toLowerCase());
		assertEquals(attrEntity.getBoolean(AttributeMetaDataMetaData.NILLABLE), Boolean.valueOf(attrNillable));
		assertEquals(attrEntity.getBoolean(AttributeMetaDataMetaData.AUTO), Boolean.valueOf(attrAuto));
		assertEquals(attrEntity.getBoolean(AttributeMetaDataMetaData.VISIBLE), Boolean.valueOf(attrVisible));
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.LABEL), attrLabel);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.LABEL + "-nl"), attrLabelNl);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.LABEL + "-en"), attrLabelEn);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.DESCRIPTION), attrDescription);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.DESCRIPTION + "-de"), attrDescriptionDe);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.DESCRIPTION + "-fr"), attrDescriptionFr);
		assertEquals(attrEntity.getBoolean(AttributeMetaDataMetaData.AGGREGATEABLE),
				Boolean.valueOf(attrAggregateable));
		assertEquals(attrEntity.getBoolean(AttributeMetaDataMetaData.READ_ONLY), Boolean.valueOf(attrReadonly));
		assertEquals(attrEntity.getBoolean(AttributeMetaDataMetaData.UNIQUE), Boolean.valueOf(attrUnique));
		assertEquals(Lists.newArrayList(attrEntity.getEntities(AttributeMetaDataMetaData.PARTS)).size(), 2);
		assertEquals(Lists.newArrayList(attrEntity.getEntities(AttributeMetaDataMetaData.TAGS)).size(), 1);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.REF_ENTITY), refEntityName);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.EXPRESSION), attrExpression);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.ENUM_OPTIONS), "a,b,c");
		assertEquals(attrEntity.getLong(AttributeMetaDataMetaData.RANGE_MIN), Long.valueOf(1l));
		assertEquals(attrEntity.getLong(AttributeMetaDataMetaData.RANGE_MAX), Long.valueOf(3l));
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.VISIBLE_EXPRESSION), attrVisibleExpression);
		assertEquals(attrEntity.getString(AttributeMetaDataMetaData.VALIDATION_EXPRESSION), attrValidationExpression);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void toEntityEntityMetaData()
	{
		AttributeMetaData idAttr = mock(AttributeMetaData.class);
		when(idAttr.getAttributeParts()).thenReturn(emptyList());
		when(idAttr.getTags()).thenReturn(emptyList());

		AttributeMetaData labelAttr = mock(AttributeMetaData.class);
		when(labelAttr.getAttributeParts()).thenReturn(emptyList());
		when(labelAttr.getTags()).thenReturn(emptyList());

		EntityMetaData extendsEntityMeta = mock(EntityMetaData.class);
		when(extendsEntityMeta.getOwnLookupAttributes()).thenReturn(emptyList());
		when(extendsEntityMeta.getOwnAttributes()).thenReturn(emptyList());
		when(extendsEntityMeta.getOwnTags()).thenReturn(emptyList());

		Package package_ = mock(Package.class);
		when(package_.getTags()).thenReturn(emptyList());

		String entitySimpleName = "name0";
		String entityBackend = "backend0";
		String entityFullName = "fullname0";
		boolean abstract_ = false;
		String label = "label0";
		String labelNl = "labelNl0";
		String labelEn = "labelEn0";
		String description = "description0";
		String descriptionDe = "descriptionDe0";
		String descriptionFr = "descriptionFr0";
		boolean system = true;

		EntityMetaData entityMeta = mock(EntityMetaData.class);

		String tagIdentifier = "id0";
		Relation tagRelation = Relation.instanceOf;
		String tagObjectIri = "objectIri";
		String tagObjectLabel = "objectLabel";
		String codeSystemIri = "codeSystemIri";
		String codeSystemLabel = "codeSystemLabel";
		Tag<EntityMetaData, LabeledResource, LabeledResource> tag0 = mock(Tag.class);
		when(tag0.getRelation()).thenReturn(tagRelation);
		when(tag0.getSubject()).thenReturn(entityMeta);
		when(tag0.getObject()).thenReturn(new LabeledResource(tagObjectIri, tagObjectLabel));
		when(tag0.getCodeSystem()).thenReturn(new LabeledResource(codeSystemIri, codeSystemLabel));
		when(tag0.getIdentifier()).thenReturn(tagIdentifier);

		when(entityMeta.getSimpleName()).thenReturn(entitySimpleName);
		when(entityMeta.getBackend()).thenReturn(entityBackend);
		when(entityMeta.getName()).thenReturn(entityFullName);
		when(entityMeta.getOwnIdAttribute()).thenReturn(idAttr);
		when(entityMeta.getOwnLabelAttribute()).thenReturn(labelAttr);
		when(entityMeta.getOwnLookupAttributes()).thenReturn(Arrays.asList(idAttr, labelAttr));
		when(entityMeta.isAbstract()).thenReturn(abstract_);
		when(entityMeta.getLabel()).thenReturn(label);
		when(entityMeta.getExtends()).thenReturn(extendsEntityMeta);
		when(entityMeta.getDescription()).thenReturn(description);
		when(entityMeta.getPackage()).thenReturn(package_);
		when(entityMeta.getOwnTags()).thenReturn(singleton(tag0));
		when(entityMeta.getOwnAttributes()).thenReturn(Arrays.asList(idAttr, labelAttr));
		when(entityMeta.isSystem()).thenReturn(system);
		when(entityMeta.getLabelLanguageCodes()).thenReturn(Sets.newHashSet("nl", "en"));
		when(entityMeta.getLabel("nl")).thenReturn(labelNl);
		when(entityMeta.getLabel("en")).thenReturn(labelEn);
		when(entityMeta.getDescriptionLanguageCodes()).thenReturn(Sets.newHashSet("de", "fr"));
		when(entityMeta.getDescription("de")).thenReturn(descriptionDe);
		when(entityMeta.getDescription("fr")).thenReturn(descriptionFr);

		Entity entityEntity = MetaUtils.toEntity(entityMeta);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.SIMPLE_NAME), entitySimpleName);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.BACKEND), entityBackend);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.FULL_NAME), entityFullName);
		assertNotNull(entityEntity.getEntity(EntityMetaDataMetaData.ID_ATTRIBUTE));
		assertNotNull(entityEntity.getEntity(EntityMetaDataMetaData.LABEL_ATTRIBUTE), "");
		assertEquals(Lists.newArrayList(entityEntity.getEntities(EntityMetaDataMetaData.LOOKUP_ATTRIBUTES)).size(), 2);
		assertEquals(entityEntity.getBoolean(EntityMetaDataMetaData.ABSTRACT), Boolean.valueOf(abstract_));
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.LABEL), label);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.LABEL + "-nl"), labelNl);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.LABEL + "-en"), labelEn);
		assertNotNull(entityEntity.getEntity(EntityMetaDataMetaData.EXTENDS));
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.DESCRIPTION), description);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.DESCRIPTION + "-de"), descriptionDe);
		assertEquals(entityEntity.getString(EntityMetaDataMetaData.DESCRIPTION + "-fr"), descriptionFr);
		assertNotNull(entityEntity.getEntity(EntityMetaDataMetaData.PACKAGE));
		assertEquals(Lists.newArrayList(entityEntity.getEntities(EntityMetaDataMetaData.TAGS)).size(), 1);
		assertEquals(Lists.newArrayList(entityEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES)).size(), 2);
		assertEquals(entityEntity.getBoolean(EntityMetaDataMetaData.SYSTEM), Boolean.valueOf(system));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void toEntityPackage()
	{
		String tagIdentifier = "id0";
		Relation tagRelation = Relation.instanceOf;
		String tagObjectIri = "objectIri";
		String tagObjectLabel = "objectLabel";
		String codeSystemIri = "codeSystemIri";
		String codeSystemLabel = "codeSystemLabel";

		Package package_ = mock(Package.class);

		Tag<Package, LabeledResource, LabeledResource> tag0 = mock(Tag.class);
		when(tag0.getRelation()).thenReturn(tagRelation);
		when(tag0.getSubject()).thenReturn(package_);
		when(tag0.getObject()).thenReturn(new LabeledResource(tagObjectIri, tagObjectLabel));
		when(tag0.getCodeSystem()).thenReturn(new LabeledResource(codeSystemIri, codeSystemLabel));
		when(tag0.getIdentifier()).thenReturn(tagIdentifier);

		String packageName0 = "name0";
		String packageSimpleName0 = "simpleName0";
		String packageDescription0 = "description0";
		Package packageParent0 = mock(Package.class);
		when(packageParent0.getTags()).thenReturn(emptyList());

		when(package_.getName()).thenReturn(packageName0);
		when(package_.getSimpleName()).thenReturn(packageSimpleName0);
		when(package_.getDescription()).thenReturn(packageDescription0);
		when(package_.getParent()).thenReturn(packageParent0);
		when(package_.getTags()).thenReturn(singletonList(tag0));

		Entity packageEntity = MetaUtils.toEntity(package_);
		assertEquals(packageEntity.getString(PackageMetaData.FULL_NAME), packageName0);
		assertEquals(packageEntity.getString(PackageMetaData.SIMPLE_NAME), packageSimpleName0);
		assertEquals(packageEntity.getString(PackageMetaData.DESCRIPTION), packageDescription0);
		assertNotNull(packageEntity.getEntity(PackageMetaData.PARENT));
		assertEquals(Lists.newArrayList(packageEntity.getEntities(PackageMetaData.TAGS)).size(), 1);
	}

	@Test
	public void toExistingAttributeMetaData()
	{
		DefaultEntityMetaData entityMetaData = new DefaultEntityMetaData("entityMetaData");
		AttributeMetaData attributeHeight = new DefaultAttributeMetaData("height_0");
		AttributeMetaData attributeWeight = new DefaultAttributeMetaData("weight_0");
		entityMetaData.addAttributeMetaData(attributeHeight);
		entityMetaData.addAttributeMetaData(attributeWeight);

		MapEntity entity1 = new MapEntity(
				ImmutableMap.of(AttributeMetaDataMetaData.NAME, "height_0", AttributeMetaDataMetaData.LABEL, "height",
						AttributeMetaDataMetaData.DESCRIPTION, "this is a height measurement in m!"));
		Iterable<Entity> attributeMetaDataEntities = Arrays.<Entity> asList(entity1);

		Iterable<AttributeMetaData> actual = MetaUtils.toExistingAttributeMetaData(entityMetaData,
				attributeMetaDataEntities);
		Iterable<AttributeMetaData> expected = Arrays.<AttributeMetaData> asList(attributeHeight);
		assertEquals(actual, expected);
	}

	@Test(expectedExceptions =
	{ MolgenisDataAccessException.class })
	public void toExistingAttributeMetaData_MolgenisDataAccessException()
	{
		DefaultEntityMetaData entityMetaData = new DefaultEntityMetaData("entityMetaData");
		AttributeMetaData attributeHeight = new DefaultAttributeMetaData("height_0");
		AttributeMetaData attributeWeight = new DefaultAttributeMetaData("weight_0");
		entityMetaData.addAttributeMetaData(attributeHeight);
		entityMetaData.addAttributeMetaData(attributeWeight);

		MapEntity entity1 = new MapEntity(
				ImmutableMap.of(AttributeMetaDataMetaData.NAME, "height_wrong_name", AttributeMetaDataMetaData.LABEL,
						"height", AttributeMetaDataMetaData.DESCRIPTION, "this is a height measurement in m!"));
		Iterable<Entity> attributeMetaDataEntities = Arrays.<Entity> asList(entity1);
		MetaUtils.toExistingAttributeMetaData(entityMetaData, attributeMetaDataEntities);
	}
}
