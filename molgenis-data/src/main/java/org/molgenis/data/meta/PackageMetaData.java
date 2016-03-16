package org.molgenis.data.meta;

import static org.molgenis.MolgenisFieldTypes.MREF;
import static org.molgenis.MolgenisFieldTypes.TEXT;
import static org.molgenis.MolgenisFieldTypes.XREF;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_LABEL;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class PackageMetaData extends SystemEntityMetaData
{
	public static final String ENTITY_NAME = "packages";
	public static final String FULL_NAME = "fullName";
	public static final String SIMPLE_NAME = "name";
	public static final String DESCRIPTION = "description";
	public static final String PARENT = "parent";
	public static final String TAGS = "tags";

	public static final PackageMetaData INSTANCE = new PackageMetaData();

	private PackageMetaData()
	{
		super(ENTITY_NAME);
		setLabel("Package");
		addAttribute(FULL_NAME, ROLE_ID, ROLE_LABEL).setLabel("Fully qualified name");
		addAttribute(SIMPLE_NAME).setLabel("Name").setNillable(false).setReadOnly(true);
		addAttribute(DESCRIPTION).setLabel("Description").setDataType(TEXT);
		addAttribute(PARENT).setLabel("Parent").setDataType(XREF).setRefEntity(this).setReadOnly(true);
		addAttribute(TAGS).setLabel("Tags").setDataType(MREF).setRefEntity(TagMetaData.INSTANCE);
	}

}
