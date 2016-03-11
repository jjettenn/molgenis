package org.molgenis.data.meta;

import static org.molgenis.MolgenisFieldTypes.XREF;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class RepositoryMetaData extends SystemEntityMetaData
{
	public static final RepositoryMetaData INSTANCE = new RepositoryMetaData();

	public static final String ENTITY_NAME = "Repository";

	public static final String ID = "id";
	public static final String COLLECTION = "collection";

	public RepositoryMetaData()
	{
		super(ENTITY_NAME);
		addAttribute(ID, ROLE_ID);
		addAttribute(COLLECTION).setDataType(XREF).setRefEntity(RepositoryCollectionMetaData.INSTANCE)
				.setNillable(false);
	}
}
