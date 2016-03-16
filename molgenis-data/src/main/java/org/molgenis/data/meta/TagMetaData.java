package org.molgenis.data.meta;

import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LABEL;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LOOKUP;

import org.molgenis.MolgenisFieldTypes;
import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class TagMetaData extends SystemEntityMetaData
{
	public static final String ENTITY_NAME = "tags";
	public static final String IDENTIFIER = "identifier";
	public static final String OBJECT_IRI = "objectIRI";
	public static final String LABEL = "label";
	public static final String RELATION_IRI = "relationIRI";
	public static final String RELATION_LABEL = "relationLabel";
	public static final String CODE_SYSTEM = "codeSystem";

	public static final TagMetaData INSTANCE = new TagMetaData();

	private TagMetaData()
	{
		super(ENTITY_NAME);
		setLabel("Tag");
		addAttribute(IDENTIFIER, ROLE_ID).setLabel("Identifier");
		addAttribute(OBJECT_IRI, ROLE_LOOKUP).setLabel("Object IRI").setDataType(MolgenisFieldTypes.TEXT);
		addAttribute(LABEL, ROLE_LABEL, ROLE_LOOKUP).setLabel("Label").setNillable(false);
		addAttribute(RELATION_IRI).setLabel("Relation IRI").setNillable(false);
		addAttribute(RELATION_LABEL).setLabel("Relation label").setNillable(false);
		addAttribute(CODE_SYSTEM).setLabel("Code system");
	}
}
