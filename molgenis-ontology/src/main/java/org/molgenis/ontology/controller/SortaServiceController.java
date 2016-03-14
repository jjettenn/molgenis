package org.molgenis.ontology.controller;

import static org.molgenis.ontology.controller.SortaServiceController.URI;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.molgenis.auth.MolgenisUser;
import org.molgenis.data.AttributeMetaData;
import org.molgenis.data.DataService;
import org.molgenis.data.Entity;
import org.molgenis.data.MolgenisInvalidFormatException;
import org.molgenis.data.Query;
import org.molgenis.data.QueryRule;
import org.molgenis.data.QueryRule.Operator;
import org.molgenis.data.Repository;
import org.molgenis.data.Sort;
import org.molgenis.data.Sort.Direction;
import org.molgenis.data.csv.CsvWriter;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.jobs.JobExecution;
import org.molgenis.data.meta.MetaValidationUtils;
import org.molgenis.data.rest.EntityCollectionResponse;
import org.molgenis.data.rest.EntityPager;
import org.molgenis.data.support.MapEntity;
import org.molgenis.data.support.QueryImpl;
import org.molgenis.file.FileStore;
import org.molgenis.ontology.core.meta.OntologyMetaData;
import org.molgenis.ontology.core.meta.OntologyTermMetaData;
import org.molgenis.ontology.core.service.OntologyService;
import org.molgenis.ontology.request.OntologyServiceRequest;
import org.molgenis.ontology.roc.MatchQualityRocService;
import org.molgenis.ontology.sorta.job.SortaJobExecution;
import org.molgenis.ontology.sorta.job.SortaJobFactory;
import org.molgenis.ontology.sorta.job.SortaJobImpl;
import org.molgenis.ontology.sorta.meta.MatchingTaskContentEntityMetaData;
import org.molgenis.ontology.sorta.meta.MatchingTaskEntityMetaData;
import org.molgenis.ontology.sorta.repo.SortaCsvRepository;
import org.molgenis.ontology.sorta.request.SortaServiceResponse;
import org.molgenis.ontology.sorta.service.SortaService;
import org.molgenis.ontology.sorta.service.impl.SortaServiceImpl;
import org.molgenis.ontology.utils.SortaServiceUtil;
import org.molgenis.security.core.MolgenisPermissionService;
import org.molgenis.security.user.UserAccountService;
import org.molgenis.ui.MolgenisPluginController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

@Controller
@RequestMapping(URI)
public class SortaServiceController extends MolgenisPluginController
{
	private final OntologyService ontologyService;
	private final SortaService sortaService;
	private final MatchQualityRocService matchQualityRocService;
	private final DataService dataService;
	private final UserAccountService userAccountService;
	private final SortaJobFactory sortaMatchJobFactory;
	private final ExecutorService taskExecutor;
	private final FileStore fileStore;
	private final MolgenisPermissionService molgenisPermissionService;
	private final LanguageService languageService;

	public static final String VIEW_NAME = "ontology-match-view";
	public static final String ID = "ontologyservice";
	public static final String URI = MolgenisPluginController.PLUGIN_URI_PREFIX + ID;
	private static final String ILLEGAL_PATTERN = "[^0-9a-zA-Z_]";
	private static final String ILLEGAL_PATTERN_REPLACEMENT = "_";
	private static final String SORTA_MATCH_JOB_TYPE = "SORTA";
	private static final double DEFAULT_THRESHOLD = 100.0;

	@Autowired
	public SortaServiceController(OntologyService ontologyService, SortaService sortaService,
			MatchQualityRocService matchQualityRocService, SortaJobFactory sortaMatchJobFactory,
			ExecutorService taskExecutor, UserAccountService userAccountService, FileStore fileStore,
			MolgenisPermissionService molgenisPermissionService, DataService dataService,
			LanguageService languageService)
	{
		super(URI);
		this.ontologyService = requireNonNull(ontologyService);
		this.sortaService = requireNonNull(sortaService);
		this.matchQualityRocService = requireNonNull(matchQualityRocService);
		this.sortaMatchJobFactory = requireNonNull(sortaMatchJobFactory);
		this.taskExecutor = requireNonNull(taskExecutor);
		this.userAccountService = requireNonNull(userAccountService);
		this.fileStore = requireNonNull(fileStore);
		this.molgenisPermissionService = requireNonNull(molgenisPermissionService);
		this.dataService = requireNonNull(dataService);
		this.languageService = requireNonNull(languageService);
	}

	@RequestMapping(method = GET)
	public String init(Model model)
	{
		model.addAttribute("existingTasks", new Iterable<Entity>()
		{
			@Override
			public Iterator<Entity> iterator()
			{
				return dataService.findAll(SortaJobExecution.ENTITY_NAME,
						QueryImpl.EQ(SortaJobExecution.USER, userAccountService.getCurrentUser())).iterator();
			}
		});

		return VIEW_NAME;
	}

	@RequestMapping(method = GET, value = "/newtask")
	public String matchTask(Model model)
	{
		model.addAttribute("ontologies", ontologyService.getOntologies());
		return VIEW_NAME;
	}

	@RequestMapping(method = RequestMethod.GET, value = "/calculate/{entityName}")
	public String calculateRoc(@PathVariable String entityName, Model model)
			throws IOException, MolgenisInvalidFormatException
	{
		model.addAllAttributes(matchQualityRocService.calculateROC(entityName));
		return init(model);
	}

	@RequestMapping(method = POST, value = "/threshold/{entityName}")
	public String updateThreshold(@RequestParam(value = "threshold", required = true) String threshold,
			@PathVariable String entityName, Model model)
	{
		if (!StringUtils.isEmpty(threshold))
		{
			Entity entity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
					new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, entityName));
			try
			{
				Double threshold_value = Double.parseDouble(threshold);
				entity.set(MatchingTaskEntityMetaData.THRESHOLD, threshold_value);
				dataService.update(MatchingTaskEntityMetaData.ENTITY_NAME, entity);
				dataService.getRepository(MatchingTaskEntityMetaData.ENTITY_NAME).flush();
			}
			catch (Exception e)
			{
				model.addAttribute("message", threshold + " is illegal threshold value!");
			}
		}

		return matchResult(entityName, model);
	}

	@RequestMapping(method = GET, value = "/result/{entityName}")
	public String matchResult(@PathVariable("entityName") String targetEntityName, Model model)
	{
		Entity entity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
				new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, targetEntityName));
		model.addAttribute("threshold", entity.get(MatchingTaskEntityMetaData.THRESHOLD));
		model.addAttribute("ontologyIri", entity.get(MatchingTaskEntityMetaData.CODE_SYSTEM));
		model.addAttribute("numberOfMatched", countMatchedEntities(targetEntityName, true));
		model.addAttribute("numberOfUnmatched", countMatchedEntities(targetEntityName, false));

		return VIEW_NAME;
	}

	@RequestMapping(method = GET, value = "/count/{entityName}")
	@ResponseBody
	public Map<String, Object> countMatchResult(@PathVariable("entityName") String entityName)
	{
		return ImmutableMap.of("numberOfMatched", countMatchedEntities(entityName, true), "numberOfUnmatched",
				countMatchedEntities(entityName, false));
	}

	@RequestMapping(method = GET, value = "/delete/{entityName}")
	@ResponseStatus(value = HttpStatus.OK)
	public String deleteResult(@PathVariable("entityName") String entityName, Model model)
	{
		if (dataService.hasRepository(entityName))
		{
			// Remove all the matching terms from MatchingTaskContentEntity table
			Stream<Entity> iterableMatchingEntities = dataService.findAll(MatchingTaskContentEntityMetaData.ENTITY_NAME,
					new QueryImpl().eq(MatchingTaskContentEntityMetaData.REF_ENTITY, entityName));
			dataService.delete(MatchingTaskContentEntityMetaData.ENTITY_NAME, iterableMatchingEntities);

			// Remove the matching task meta information from MatchingTaskEntity table
			Entity matchingSummaryEntity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
					new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, entityName));
			dataService.delete(MatchingTaskEntityMetaData.ENTITY_NAME, matchingSummaryEntity);

			// Drop the table that contains the information for raw data (input terms)
			dataService.getMeta().deleteEntityMeta(entityName);

			dataService.getRepository(MatchingTaskEntityMetaData.ENTITY_NAME).flush();

			Entity jobEntity = dataService.findOne(SortaJobExecution.ENTITY_NAME,
					QueryImpl.EQ(SortaJobExecution.USER, userAccountService.getCurrentUser()).and()
							.eq(SortaJobExecution.TARGET_ENTITY, entityName));
			dataService.delete(SortaJobExecution.ENTITY_NAME, jobEntity);

			dataService.getRepository(SortaJobExecution.ENTITY_NAME).flush();
		}
		return init(model);
	}

	@RequestMapping(method = POST, value = "/match/retrieve")
	@ResponseBody
	public EntityCollectionResponse matchResult(@RequestBody OntologyServiceRequest ontologyServiceRequest,
			HttpServletRequest httpServletRequest)
	{
		List<Map<String, Object>> entityMaps = new ArrayList<Map<String, Object>>();
		String entityName = ontologyServiceRequest.getEntityName();
		String filterQuery = ontologyServiceRequest.getFilterQuery();
		String ontologyIri = ontologyServiceRequest.getOntologyIri();
		EntityPager entityPager = ontologyServiceRequest.getEntityPager();
		boolean isMatched = ontologyServiceRequest.isMatched();
		Entity entity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
				new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, entityName));
		Double threshold = Double.parseDouble(entity.get(MatchingTaskEntityMetaData.THRESHOLD).toString());

		QueryRule queryRuleInputEntities = new QueryRule(
				Arrays.asList(new QueryRule(MatchingTaskContentEntityMetaData.VALIDATED, Operator.EQUALS, isMatched),
						new QueryRule(isMatched ? Operator.OR : Operator.AND),
						new QueryRule(MatchingTaskContentEntityMetaData.SCORE,
								isMatched ? Operator.GREATER_EQUAL : Operator.LESS, threshold)));

		QueryRule queryRuleMatchingTask = new QueryRule(MatchingTaskContentEntityMetaData.REF_ENTITY, Operator.EQUALS,
				entityName);

		List<QueryRule> queryRuleInputEntitiesInOneMatchingTask = Arrays.asList(queryRuleMatchingTask,
				new QueryRule(Operator.AND), queryRuleInputEntities);

		// Add filter to the query if query string is not empty
		if (StringUtils.isNotEmpty(filterQuery))
		{
			Iterable<String> filteredInputTermIds = dataService.findAll(entityName, new QueryImpl().search(filterQuery))
					.map(inputEntity -> inputEntity.getString(SortaServiceImpl.DEFAULT_MATCHING_IDENTIFIER))
					.collect(Collectors.toList());
			QueryRule previousQueryRule = new QueryRule(queryRuleInputEntitiesInOneMatchingTask);
			QueryRule queryRuleFilterInput = new QueryRule(MatchingTaskContentEntityMetaData.INPUT_TERM, Operator.IN,
					filteredInputTermIds);
			queryRuleInputEntitiesInOneMatchingTask = Arrays.asList(previousQueryRule, new QueryRule(Operator.AND),
					queryRuleFilterInput);
		}

		Query query = new QueryImpl(queryRuleInputEntitiesInOneMatchingTask);
		long count = dataService.count(MatchingTaskContentEntityMetaData.ENTITY_NAME, query);
		int start = entityPager.getStart();
		int num = entityPager.getNum();

		Stream<Entity> findAll = dataService.findAll(MatchingTaskContentEntityMetaData.ENTITY_NAME,
				query.offset(start).pageSize(num)
						.sort(new Sort().on(MatchingTaskContentEntityMetaData.VALIDATED, Direction.DESC)
								.on(MatchingTaskContentEntityMetaData.SCORE, Direction.DESC)));
		findAll.forEach(mappingEntity -> {
			Entity RefEntity = dataService.findOne(entityName, new QueryImpl().eq(SortaCsvRepository.ALLOWED_IDENTIFIER,
					mappingEntity.getString(MatchingTaskContentEntityMetaData.INPUT_TERM)));
			Map<String, Object> outputEntity = new HashMap<String, Object>();
			outputEntity.put("inputTerm", SortaServiceUtil.getEntityAsMap(RefEntity));
			outputEntity.put("matchedTerm", SortaServiceUtil.getEntityAsMap(mappingEntity));
			Object matchedTerm = mappingEntity.get(MatchingTaskContentEntityMetaData.MATCHED_TERM);
			if (matchedTerm != null)
			{
				outputEntity.put("ontologyTerm", SortaServiceUtil
						.getEntityAsMap(sortaService.getOntologyTermEntity(matchedTerm.toString(), ontologyIri)));
			}
			entityMaps.add(outputEntity);
		});

		EntityPager pager = new EntityPager(start, num, count, null);
		return new EntityCollectionResponse(pager, entityMaps, "/match/retrieve", OntologyTermMetaData.INSTANCE,
				molgenisPermissionService, dataService, languageService);
	}

	@RequestMapping(method = POST, value = "/match")
	public String match(@RequestParam(value = "taskName", required = true) String entityName,
			@RequestParam(value = "selectOntologies", required = true) String ontologyIri,
			@RequestParam(value = "inputTerms", required = true) String inputTerms, Model model,
			HttpServletRequest httpServletRequest) throws Exception
	{
		if (StringUtils.isEmpty(ontologyIri) || StringUtils.isEmpty(inputTerms)) return init(model);
		String sessionId = httpServletRequest.getSession().getId();
		File uploadFile = fileStore.store(new ByteArrayInputStream(inputTerms.getBytes("UTF8")),
				sessionId + "_input.txt");
		return startMatchJob(entityName, ontologyIri, uploadFile, model);
	}

	@RequestMapping(method = POST, value = "/match/upload", headers = "Content-Type=multipart/form-data")
	public String upload(@RequestParam(value = "taskName", required = true) String entityName,
			@RequestParam(value = "selectOntologies", required = true) String ontologyIri,
			@RequestParam(value = "file", required = true) Part file, Model model,
			HttpServletRequest httpServletRequest) throws Exception
	{
		if (StringUtils.isEmpty(ontologyIri) || file == null) return init(model);
		String sessionId = httpServletRequest.getSession().getId();
		File uploadFile = fileStore.store(file.getInputStream(), sessionId + "_input.csv");
		return startMatchJob(entityName, ontologyIri, uploadFile, model);
	}

	@RequestMapping(method = POST, value = "/match/entity")
	@ResponseBody
	public SortaServiceResponse matchResult(@RequestBody Map<String, Object> request,
			HttpServletRequest httpServletRequest)
	{
		if (request.containsKey("entityName") && !StringUtils.isEmpty(request.get("entityName").toString())
				&& request.containsKey(MatchingTaskContentEntityMetaData.IDENTIFIER)
				&& !StringUtils.isEmpty(request.get(MatchingTaskContentEntityMetaData.IDENTIFIER).toString()))
		{
			String entityName = request.get("entityName").toString();
			String inputTermIdentifier = request.get(MatchingTaskContentEntityMetaData.IDENTIFIER).toString();
			Entity matchingTaskEntity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
					new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, entityName));
			Entity inputEntity = dataService.findOne(entityName,
					new QueryImpl().eq(MatchingTaskContentEntityMetaData.IDENTIFIER, inputTermIdentifier));

			if (matchingTaskEntity == null || inputEntity == null)
				return new SortaServiceResponse("entityName or inputTermIdentifier is invalid!");

			return new SortaServiceResponse(inputEntity, sortaService.findOntologyTermEntities(
					matchingTaskEntity.getString(MatchingTaskEntityMetaData.CODE_SYSTEM), inputEntity));
		}
		return new SortaServiceResponse("Please check entityName, inputTermIdentifier exist in input!");
	}

	@RequestMapping(method = POST, value = "/search")
	@ResponseBody
	public SortaServiceResponse search(@RequestBody Map<String, Object> request, HttpServletRequest httpServletRequest)
	{
		if (request.containsKey("queryString") && !StringUtils.isEmpty(request.get("queryString").toString())
				&& request.containsKey(OntologyMetaData.ONTOLOGY_IRI)
				&& !StringUtils.isEmpty(request.get(OntologyMetaData.ONTOLOGY_IRI).toString()))
		{
			String queryString = request.get("queryString").toString();
			String ontologyIri = request.get(OntologyMetaData.ONTOLOGY_IRI).toString();
			Entity inputEntity = new MapEntity(
					Collections.singletonMap(SortaServiceImpl.DEFAULT_MATCHING_NAME_FIELD, queryString));

			return new SortaServiceResponse(inputEntity,
					sortaService.findOntologyTermEntities(ontologyIri, inputEntity));
		}
		return new SortaServiceResponse("Please check entityName, inputTermIdentifier exist in input!");
	}

	@RequestMapping(method = GET, value = "/match/download/{entityName}")
	public void download(@PathVariable String entityName, HttpServletResponse response, Model model) throws IOException
	{
		CsvWriter csvWriter = new CsvWriter(response.getOutputStream(), SortaServiceImpl.DEFAULT_SEPARATOR);
		try
		{
			response.setContentType("text/csv");
			response.addHeader("Content-Disposition", "attachment; filename=" + generateCsvFileName("match-result"));
			List<String> columnHeaders = new ArrayList<String>();
			for (AttributeMetaData attributeMetaData : dataService.getEntityMetaData(entityName).getAttributes())
			{
				if (!attributeMetaData.getName().equalsIgnoreCase(MatchingTaskEntityMetaData.IDENTIFIER))
					columnHeaders.add(attributeMetaData.getName());
			}
			columnHeaders.addAll(
					Arrays.asList(OntologyTermMetaData.ONTOLOGY_TERM_NAME, OntologyTermMetaData.ONTOLOGY_TERM_IRI,
							MatchingTaskContentEntityMetaData.SCORE, MatchingTaskContentEntityMetaData.VALIDATED));
			csvWriter.writeAttributeNames(columnHeaders);

			Entity matchingTaskEntity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
					new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, entityName));

			dataService
					.findAll(MatchingTaskContentEntityMetaData.ENTITY_NAME,
							new QueryImpl().eq(MatchingTaskContentEntityMetaData.REF_ENTITY, entityName))
					.forEach(mappingEntity -> {
						Entity inputEntity = dataService.findOne(entityName,
								new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER,
										mappingEntity.getString(MatchingTaskContentEntityMetaData.INPUT_TERM)));
						Entity ontologyTermEntity = sortaService.getOntologyTermEntity(
								mappingEntity.getString(MatchingTaskContentEntityMetaData.MATCHED_TERM),
								matchingTaskEntity.getString(MatchingTaskEntityMetaData.CODE_SYSTEM));
						MapEntity row = new MapEntity(inputEntity);
						row.set(OntologyTermMetaData.ONTOLOGY_TERM_NAME,
								ontologyTermEntity.get(OntologyTermMetaData.ONTOLOGY_TERM_NAME));
						row.set(OntologyTermMetaData.ONTOLOGY_TERM_IRI,
								ontologyTermEntity.get(OntologyTermMetaData.ONTOLOGY_TERM_IRI));
						row.set(MatchingTaskContentEntityMetaData.VALIDATED,
								mappingEntity.get(MatchingTaskContentEntityMetaData.VALIDATED));
						row.set(MatchingTaskContentEntityMetaData.SCORE,
								mappingEntity.get(MatchingTaskContentEntityMetaData.SCORE));
						csvWriter.add(row);
					});
		}
		finally
		{
			if (csvWriter != null) IOUtils.closeQuietly(csvWriter);
		}
	}

	private String startMatchJob(String targetEntityName, String ontologyIri, File uploadedFile, Model model)
			throws IOException
	{
		targetEntityName = targetEntityName.replaceAll(ILLEGAL_PATTERN, ILLEGAL_PATTERN_REPLACEMENT).toLowerCase();

		MetaValidationUtils.validateName(targetEntityName);

		if (dataService.hasRepository(targetEntityName))
		{
			Entity matchingTaskEntity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
					new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, targetEntityName));
			model.addAttribute("message",
					"The task name should be case insensitive, the task name <strong>" + targetEntityName
							+ "</strong> has existed and created by user : "
							+ (matchingTaskEntity != null
									? matchingTaskEntity.get(MatchingTaskEntityMetaData.MOLGENIS_USER)
									: StringUtils.EMPTY));
			return init(model);
		}
		SortaCsvRepository repository = new SortaCsvRepository(targetEntityName, uploadedFile);

		if (!validateFileHeader(repository))
		{
			model.addAttribute("message", "The Name header is missing!");
			return matchTask(model);
		}

		if (!validateEmptyFileHeader(repository))
		{
			model.addAttribute("message", "The empty header is not allowed!");
			return matchTask(model);
		}

		if (!validateInputFileContent(repository))
		{
			model.addAttribute("message", "The content of input is empty!");
			return matchTask(model);
		}

		JobExecution jobExecution = createJobExecution(repository, ontologyIri);
		MolgenisUser currentUser = userAccountService.getCurrentUser();
		SortaJobImpl sortaMatchJob = sortaMatchJobFactory.create(ontologyIri, repository.getName(), currentUser,
				jobExecution, SecurityContextHolder.getContext());
		taskExecutor.submit(sortaMatchJob);

		return init(model);
	}

	private JobExecution createJobExecution(Repository repository, String ontologyIri)
	{
		// Add the original input dataset to database
		dataService.getMeta().addEntityMeta(repository.getEntityMetaData());
		dataService.getRepository(repository.getName()).add(repository.stream());
		dataService.getRepository(repository.getName()).flush();

		// Add a new entry in MatchingTask table for this new matching job
		MapEntity mapEntity = new MapEntity();
		mapEntity.set(MatchingTaskEntityMetaData.IDENTIFIER, repository.getName());
		mapEntity.set(MatchingTaskEntityMetaData.DATA_CREATED, new Date());
		mapEntity.set(MatchingTaskEntityMetaData.CODE_SYSTEM, ontologyIri);
		mapEntity.set(MatchingTaskEntityMetaData.MOLGENIS_USER, userAccountService.getCurrentUser().getUsername());
		mapEntity.set(MatchingTaskEntityMetaData.THRESHOLD, DEFAULT_THRESHOLD);
		dataService.add(MatchingTaskEntityMetaData.ENTITY_NAME, mapEntity);
		dataService.getRepository(MatchingTaskEntityMetaData.ENTITY_NAME).flush();

		// Create a Sorta Job Execution
		SortaJobExecution jobExecution = new SortaJobExecution(dataService);
		jobExecution.setUser(userAccountService.getCurrentUser());
		jobExecution.setType(SORTA_MATCH_JOB_TYPE);
		jobExecution.setResultUrl("/menu/main/ontologyservice/result/" + repository.getName());
		jobExecution.setDeleteUrl("/menu/main/ontologyservice/delete/" + repository.getName());
		jobExecution.setTargetEntityName(repository.getName());
		jobExecution.setOntologyIri(ontologyIri);
		dataService.add(SortaJobExecution.ENTITY_NAME, jobExecution);
		return jobExecution;
	}

	private long countMatchedEntities(String entityName, boolean isMatched)
	{
		Entity entity = dataService.findOne(MatchingTaskEntityMetaData.ENTITY_NAME,
				new QueryImpl().eq(MatchingTaskEntityMetaData.IDENTIFIER, entityName));

		double threshold = entity.getDouble(MatchingTaskEntityMetaData.THRESHOLD);

		QueryRule rule_1 = new QueryRule(MatchingTaskContentEntityMetaData.REF_ENTITY, Operator.EQUALS, entityName);

		QueryRule rule_2 = new QueryRule(MatchingTaskContentEntityMetaData.VALIDATED, Operator.EQUALS, isMatched);

		QueryRule rule_3 = new QueryRule(MatchingTaskContentEntityMetaData.SCORE,
				isMatched ? Operator.GREATER_EQUAL : Operator.LESS, threshold);

		QueryRule combinedRule = new QueryRule(
				Arrays.asList(rule_2, new QueryRule(isMatched ? Operator.OR : Operator.AND), rule_3));

		return dataService.count(MatchingTaskContentEntityMetaData.ENTITY_NAME,
				new QueryImpl(Arrays.asList(rule_1, new QueryRule(Operator.AND), combinedRule)));
	}

	private String generateCsvFileName(String dataSetName)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dataSetName + "_" + dateFormat.format(new Date()) + ".csv";
	}

	private boolean validateFileHeader(Repository repository)
	{
		boolean containsName = StreamSupport.stream(repository.getEntityMetaData().getAttributes().spliterator(), false)
				.map(AttributeMetaData::getName)
				.anyMatch(name -> name.equalsIgnoreCase(SortaServiceImpl.DEFAULT_MATCHING_NAME_FIELD));
		return containsName;
	}

	private boolean validateEmptyFileHeader(Repository repository)
	{
		boolean evaluation = StreamSupport.stream(repository.getEntityMetaData().getAttributes().spliterator(), false)
				.map(AttributeMetaData::getName).anyMatch(StringUtils::isNotBlank);
		return evaluation;
	}

	private boolean validateInputFileContent(Repository repository)
	{
		return repository.iterator().hasNext();
	}
}