package org.molgenis.script;

import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class ScriptParameterMetaData extends SystemEntityMetaData
{
	public ScriptParameterMetaData()
	{
		super(ScriptParameter.ENTITY_NAME, ScriptParameter.class);
		addAttribute(ScriptParameter.NAME, ROLE_ID).setNillable(false);
	}
}
