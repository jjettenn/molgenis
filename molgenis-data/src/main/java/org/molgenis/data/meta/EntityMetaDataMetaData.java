package org.molgenis.data.meta;

import static org.molgenis.MolgenisFieldTypes.BOOL;
import static org.molgenis.MolgenisFieldTypes.MREF;
import static org.molgenis.MolgenisFieldTypes.TEXT;
import static org.molgenis.MolgenisFieldTypes.XREF;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LABEL;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LOOKUP;

import org.molgenis.data.support.DefaultEntityMetaData;

public class EntityMetaDataMetaData extends DefaultEntityMetaData
{
	public static final String ENTITY_NAME = "entities";
	public static final String SIMPLE_NAME = "simpleName";
	public static final String BACKEND = "backend";
	public static final String FULL_NAME = "fullName";
	public static final String ID_ATTRIBUTE = "idAttribute";
	public static final String LABEL_ATTRIBUTE = "labelAttribute";
	public static final String LOOKUP_ATTRIBUTES = "lookupAttributes";
	public static final String ABSTRACT = "abstract";
	public static final String LABEL = "label";
	public static final String EXTENDS = "extends";
	public static final String DESCRIPTION = "description";
	public static final String PACKAGE = "package";
	public static final String TAGS = "tags";
	public static final String ATTRIBUTES = "attributes";

	public static final EntityMetaDataMetaData INSTANCE = new EntityMetaDataMetaData();

	private EntityMetaDataMetaData()
	{
		super(ENTITY_NAME);
		addAttribute(FULL_NAME, ROLE_ID).setUnique(true);
		addAttribute(SIMPLE_NAME, ROLE_LABEL).setNillable(false).setReadOnly(true);
		addAttribute(BACKEND).setReadOnly(true);
		addAttribute(PACKAGE).setDataType(XREF).setRefEntity(PackageRepository.META_DATA).setReadOnly(true);
		addAttribute(ID_ATTRIBUTE).setDataType(XREF).setRefEntity(AttributeMetaDataMetaData.INSTANCE).setReadOnly(true);
		addAttribute(LABEL_ATTRIBUTE).setDataType(XREF).setRefEntity(AttributeMetaDataMetaData.INSTANCE)
				.setReadOnly(true);
		addAttribute(LOOKUP_ATTRIBUTES).setDataType(MREF).setRefEntity(AttributeMetaDataMetaData.INSTANCE)
				.setReadOnly(true);
		addAttribute(ABSTRACT).setDataType(BOOL).setReadOnly(true);
		addAttribute(LABEL, ROLE_LOOKUP);
		addAttribute(EXTENDS).setDataType(XREF).setRefEntity(this).setReadOnly(true);
		addAttribute(DESCRIPTION, ROLE_LOOKUP).setDataType(TEXT);
		addAttribute(TAGS).setDataType(MREF).setRefEntity(TagMetaData.INSTANCE).setReadOnly(true);
		addAttribute(ATTRIBUTES).setDataType(MREF).setRefEntity(AttributeMetaDataMetaData.INSTANCE).setReadOnly(true);
	}
}
