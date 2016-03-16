package org.molgenis.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.molgenis.data.DataService;
import org.molgenis.data.Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.stream.JsonReader;

@Service
public class ScaleusSampleExporterService
{
	private static final String SCALEUS_HOST = "http://localhost:8090/scaleus";
	private final DataService dataService;
	private final HttpClient httpClient;

	@Autowired
	public ScaleusSampleExporterService(DataService dataService, HttpClient httpClient)
	{
		this.dataService = requireNonNull(dataService);
		this.httpClient = requireNonNull(httpClient);
	}

	public void exportSamples() throws ClientProtocolException, IOException
	{
		HttpGet request = new HttpGet(SCALEUS_HOST + "/api/v1/dataset/");
		request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
		List<String> datasets = httpClient.execute(request, new JsonResponseHandler<List<String>>()
		{

			@Override
			public List<String> deserialize(JsonReader jsonReader) throws IOException
			{
				List<String> datasets = new ArrayList<String>();
				jsonReader.beginArray();
				while (jsonReader.hasNext())
				{
					datasets.add(jsonReader.nextString());
				}
				jsonReader.endArray();
				return datasets;
			}
		});
		if (datasets.contains("sample"))
		{
			httpClient.execute(new HttpDelete(SCALEUS_HOST + "/api/v1/dataset/sample"));
		}
		httpClient.execute(new HttpPost(SCALEUS_HOST + "/api/v1/dataset/sample"));

		Repository sampleRepo = dataService.getRepository("rdconnect_Sample");
		sampleRepo.forEach(sampleEntity -> {
			String sampleID = sampleEntity.getString("ID");

			StringBuilder jsonBuilder = new StringBuilder();
			jsonBuilder.append('{');
			jsonBuilder.append("\"s\":").append("http://www.molgenis.org/");
			jsonBuilder.append("\"p\":").append("http://www.molgenis.org/sampleId");
			jsonBuilder.append("\"o\":").append(sampleID);
			jsonBuilder.append('}');

			HttpPost postRequest = new HttpPost(SCALEUS_HOST + "/api/v1/store/sample");
			postRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			try
			{
				httpClient.execute(postRequest);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		});
	}

	private static abstract class JsonResponseHandler<T> implements ResponseHandler<T>
	{
		@Override
		public T handleResponse(final HttpResponse response) throws ClientProtocolException, IOException
		{
			StatusLine statusLine = response.getStatusLine();
			if (statusLine.getStatusCode() < 100 || statusLine.getStatusCode() >= 300)
			{
				throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
			}

			HttpEntity entity = response.getEntity();
			if (entity == null)
			{
				throw new ClientProtocolException("Response contains no content");
			}

			JsonReader jsonReader = new JsonReader(new InputStreamReader(entity.getContent(), UTF_8));
			try
			{
				return deserialize(jsonReader);
			}
			finally
			{
				jsonReader.close();
			}
		}

		public abstract T deserialize(JsonReader jsonReader) throws IOException;
	}
}