package org.molgenis.data.meta;

import static org.molgenis.MolgenisFieldTypes.BOOL;
import static org.molgenis.MolgenisFieldTypes.MREF;
import static org.molgenis.MolgenisFieldTypes.SCRIPT;
import static org.molgenis.MolgenisFieldTypes.TEXT;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LABEL;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LOOKUP;

import org.molgenis.MolgenisFieldTypes;
import org.molgenis.data.support.SystemEntityMetaData;
import org.molgenis.fieldtypes.EnumField;
import org.molgenis.fieldtypes.LongField;

public class AttributeMetaDataMetaData extends SystemEntityMetaData
{
	public static final String ENTITY_NAME = "attrs";
	public static final String IDENTIFIER = "identifier";
	public static final String NAME = "name";
	public static final String DATA_TYPE = "dataType";
	public static final String REF_ENTITY = "refEntity";
	public static final String EXPRESSION = "expression";
	public static final String NILLABLE = "nillable";
	public static final String AUTO = "auto";
	public static final String VISIBLE = "visible";
	public static final String LABEL = "label";
	public static final String DESCRIPTION = "description";
	public static final String AGGREGATEABLE = "aggregateable";
	public static final String ENUM_OPTIONS = "enumOptions";
	public static final String RANGE_MIN = "rangeMin";
	public static final String RANGE_MAX = "rangeMax";
	public static final String READ_ONLY = "readOnly";
	public static final String UNIQUE = "unique_";
	public static final String PARTS = "parts";
	public static final String TAGS = "tags";
	public static final String VISIBLE_EXPRESSION = "visibleExpression";
	public static final String VALIDATION_EXPRESSION = "validationExpression";
	public static final String DEFAULT_VALUE = "defaultValue";

	public static final AttributeMetaDataMetaData INSTANCE = new AttributeMetaDataMetaData();

	private AttributeMetaDataMetaData()
	{
		super(ENTITY_NAME);
		setLabel("Attribute");
		addAttribute(IDENTIFIER, ROLE_ID).setLabel("Identifier").setVisible(false);
		addAttribute(NAME, ROLE_LABEL, ROLE_LOOKUP).setLabel("Name").setNillable(false).setReadOnly(true);
		addAttribute(DATA_TYPE).setLabel("Data type").setDataType(new EnumField())
				.setEnumOptions(MolgenisFieldTypes.getTypeNames()).setNillable(false);
		addAttribute(PARTS).setLabel("Parts").setDataType(MREF).setRefEntity(this).setReadOnly(true);
		addAttribute(REF_ENTITY).setLabel("Reference entity").setReadOnly(true);
		addAttribute(EXPRESSION).setLabel("Expression").setNillable(true);
		addAttribute(NILLABLE).setLabel("Nillable").setDataType(BOOL).setNillable(false).setReadOnly(true);
		addAttribute(AUTO).setLabel("Auto").setDataType(BOOL).setNillable(false);
		addAttribute(VISIBLE).setLabel("Visible").setDataType(BOOL).setNillable(false);
		addAttribute(LABEL, ROLE_LOOKUP).setLabel("Label");
		addAttribute(DESCRIPTION).setLabel("Description").setDataType(TEXT);
		addAttribute(AGGREGATEABLE).setLabel("Aggregateable").setDataType(BOOL).setNillable(false);
		addAttribute(ENUM_OPTIONS).setLabel("Enum values").setDataType(TEXT).setReadOnly(true);
		addAttribute(RANGE_MIN).setLabel("Range (min)").setDataType(new LongField()).setReadOnly(true);
		addAttribute(RANGE_MAX).setLabel("Range (max)").setDataType(new LongField()).setReadOnly(true);
		addAttribute(READ_ONLY).setLabel("Read-only").setDataType(BOOL).setNillable(false);
		addAttribute(UNIQUE).setLabel("Unique").setDataType(BOOL).setNillable(false).setReadOnly(true);
		addAttribute(TAGS).setLabel("Tags").setDataType(MREF).setRefEntity(TagMetaData.INSTANCE);
		addAttribute(VISIBLE_EXPRESSION).setLabel("Visible expression").setDataType(SCRIPT).setNillable(true);
		addAttribute(VALIDATION_EXPRESSION).setLabel("Validation expression").setDataType(SCRIPT).setNillable(true);
		addAttribute(DEFAULT_VALUE).setLabel("Default value").setDataType(TEXT).setNillable(true).setReadOnly(true);
	}
}
