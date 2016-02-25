package org.molgenis.data.importer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.molgenis.framework.db.EntitiesValidationReport;

/**
 * Created by mswertz on 03/05/14.
 */
public class EntitiesValidationReportImpl implements EntitiesValidationReport
{
	/**
	 * map of all sheets, and whether they are importable (recognized) or not
	 */
	private final Map<String, Boolean> sheetsImportable;
	/** map of importable sheets and their importable fields */
	private final Map<String, Collection<String>> fieldsImportable;
	/** map of importable sheets and their unknown fields */
	private final Map<String, Collection<String>> fieldsUnknown;
	/** map of importable sheets and their required/missing fields */
	private final Map<String, Collection<String>> fieldsRequired;
	/** map of importable sheets and their available/optional fields */
	private final Map<String, Collection<String>> fieldsAvailable;
	/** import order of the sheets */
	private final List<String> importOrder;
	private final List<String> packages;

	public EntitiesValidationReportImpl()
	{
		this.sheetsImportable = new LinkedHashMap<>();
		this.fieldsImportable = new LinkedHashMap<>();
		this.fieldsUnknown = new LinkedHashMap<>();
		this.fieldsRequired = new LinkedHashMap<>();
		this.fieldsAvailable = new LinkedHashMap<>();
		importOrder = new ArrayList<>();
		packages = new ArrayList<>();
	}

	@Override
	public Map<String, Boolean> getSheetsImportable()
	{
		return sheetsImportable;
	}

	@Override
	public Map<String, Collection<String>> getFieldsImportable()
	{
		return fieldsImportable;
	}

	@Override
	public Map<String, Collection<String>> getFieldsUnknown()
	{
		return fieldsUnknown;
	}

	@Override
	public Map<String, Collection<String>> getFieldsRequired()
	{
		return fieldsRequired;
	}

	@Override
	public Map<String, Collection<String>> getFieldsAvailable()
	{
		return fieldsAvailable;
	}

	@Override
	public List<String> getImportOrder()
	{
		return importOrder;
	}

	@Override
	public boolean valid()
	{// determine if validation succeeded
		boolean ok = true;
		if (sheetsImportable != null)
		{
			for (Boolean b : sheetsImportable.values())
			{
				ok = ok & b;
			}

			for (Collection<String> fields : getFieldsRequired().values())
			{
				ok = ok & (fields == null || fields.isEmpty());
			}
		}
		return ok;
	}

	@Override
	public List<String> getPackages()
	{
		return packages;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fieldsAvailable == null) ? 0 : fieldsAvailable.hashCode());
		result = prime * result + ((fieldsImportable == null) ? 0 : fieldsImportable.hashCode());
		result = prime * result + ((fieldsRequired == null) ? 0 : fieldsRequired.hashCode());
		result = prime * result + ((fieldsUnknown == null) ? 0 : fieldsUnknown.hashCode());
		result = prime * result + ((importOrder == null) ? 0 : importOrder.hashCode());
		result = prime * result + ((packages == null) ? 0 : packages.hashCode());
		result = prime * result + ((sheetsImportable == null) ? 0 : sheetsImportable.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		EntitiesValidationReportImpl other = (EntitiesValidationReportImpl) obj;
		if (fieldsAvailable == null)
		{
			if (other.fieldsAvailable != null) return false;
		}
		else if (!fieldsAvailable.equals(other.fieldsAvailable)) return false;
		if (fieldsImportable == null)
		{
			if (other.fieldsImportable != null) return false;
		}
		else if (!fieldsImportable.equals(other.fieldsImportable)) return false;
		if (fieldsRequired == null)
		{
			if (other.fieldsRequired != null) return false;
		}
		else if (!fieldsRequired.equals(other.fieldsRequired)) return false;
		if (fieldsUnknown == null)
		{
			if (other.fieldsUnknown != null) return false;
		}
		else if (!fieldsUnknown.equals(other.fieldsUnknown)) return false;
		if (importOrder == null)
		{
			if (other.importOrder != null) return false;
		}
		else if (!importOrder.equals(other.importOrder)) return false;
		if (packages == null)
		{
			if (other.packages != null) return false;
		}
		else if (!packages.equals(other.packages)) return false;
		if (sheetsImportable == null)
		{
			if (other.sheetsImportable != null) return false;
		}
		else if (!sheetsImportable.equals(other.sheetsImportable)) return false;
		return true;
	}
}
