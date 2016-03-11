package org.molgenis.data.meta;

import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class RepositoryCollectionMetaData extends SystemEntityMetaData
{
	public static final RepositoryCollectionMetaData INSTANCE = new RepositoryCollectionMetaData();

	public static final String ENTITY_NAME = "RepositoryCollection";

	public static final String ID = "id";

	public RepositoryCollectionMetaData()
	{
		super(ENTITY_NAME);
		addAttribute(ID, ROLE_ID);
	}
}
