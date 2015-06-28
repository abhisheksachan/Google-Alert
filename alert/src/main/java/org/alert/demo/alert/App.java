package org.alert.demo.alert;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
import org.nnh.bean.Alert;
import org.nnh.bean.DeliveryTo;
import org.nnh.service.GAService;
public class App {
	
	public static void main(String[] args)
	{
		GAService service = null;
		Map<String,Object> doc= new HashMap<String,Object>();
		
		
		 Settings settings = ImmutableSettings
					.settingsBuilder()
					.put("cluster.name","prophesee1")
					.build();
		    
		    
		    TransportClient transportClient = new TransportClient(settings);
		    transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost",9300));
		    BulkRequestBuilder bulkRequest = transportClient.prepareBulk(); 
		    
		    
		try {
			service = new GAService("abhisheksachan94@gmail.com", "Abhishek1.");
			service.doLogin();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* 
	 ---list---org.nnh.bean.Alert@69d9c55 narendra modi
	---list---org.nnh.bean.Alert@13a57a3b
	---list---org.nnh.bean.Alert@7ca48474
	---list---org.nnh.bean.Alert@337d0578
	---list---org.nnh.bean.Alert@59e84876
*/
		Alert alert = null;
		String alertId;
		
		// Get all alerts.
		List<Alert> lstAlert = service.getAlerts();
	//	List<Alert> lstAlert = service.getAlertByDelivery(DeliveryTo.FEED);
	//	Alert alert = service.getAlertById("@69d9c55");   not working.....!!
	//	List<Alert> lstAlert = service.getAlerts("narendra modi");
		for(int i=0;i<lstAlert.size();i++)
		{
			alert = lstAlert.get(i);
			System.out.println("---list---"+alert);
			
			System.out.println("---"+alert.getDeliveryTo());
			System.out.println("---"+alert.getHowMany());
			System.out.println("---"+alert.getHowOften());
			System.out.println("---"+alert.getId());
			System.out.println("---"+alert.getLanguage());
			System.out.println("---"+alert.getRegion());
			System.out.println("---"+alert.getSearchQuery());
			System.out.println("---"+alert.getsources());
			
			
		}
		int PRETTY_PRINT_INDENT_FACTOR = 4;
		HttpURLConnection connection = null;
		String mediaData = null;
		String jobUrl = alert.getDeliveryTo();
		URL url;
		try {
			url = new URL(jobUrl);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			InputStream responseStream = connection.getInputStream();
			
		    String charset = "UTF-8";
		    mediaData = IOUtils.toString(responseStream, charset);
		    
		    System.out.println("------------get---------"+mediaData);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		org.json.JSONObject xmlJSONObj = null;
		try {
			 xmlJSONObj = XML.toJSONObject(mediaData);
			String jsonPrettyPrintString = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR);
			System.out.println(jsonPrettyPrintString);
			}
		catch (JSONException je) {
						System.out.println(je.toString());
			}
		
		
		try {
			JSONObject feed= (JSONObject) xmlJSONObj.get("feed");
			JSONArray entry=(JSONArray) feed.get("entry");
			for(int i=0;i<entry.length();i++)
			{
				JSONObject object =(JSONObject) entry.get(i);
			String id= (String)object.get("id");
			doc.put("id", id);
			String published= (String)object.get("published");
			doc.put("published",published);
			String updated= (String)object.get("updated");
			doc.put("updated",updated );
			JSONObject lin=(JSONObject) object.get("link");
			String link=(String) lin.get("href");
			doc.put("link",link );
			JSONObject author=(JSONObject) object.get("author");
			String name=(String) author.get("name");
			doc.put("name",name );
			JSONObject title=(JSONObject) object.get("title");
			String title_content=(String) title.get("content");
			doc.put("title",title_content );
			JSONObject cont=(JSONObject) object.get("content");
			String content=(String) cont.get("content");
			doc.put("content",content );
			
			String xmlns=(String) feed.get("xmlns");
			String xmlns_index=(String) feed.get("xmlns:idx");
			String id1=(String) feed.get("id");
			String title1=(String) feed.get("title");
			String update=(String) feed.get("updated");
			
			JSONObject link1 =(JSONObject) feed.get("link");
			String href=(String) link1.get("href");
			
			String s= String.valueOf(i);
			bulkRequest.add(
					transportClient
					.prepareIndex("alert","search", id)
				    .setSource(doc));
			 	
			
			
			
		if(bulkRequest.request().requests().size() == 0){
				
				System.out.println("\n\n No request Added!");
			
			} else{
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				
				if (bulkResponse.hasFailures()) {
				    System.out.println("ElasticSearch Failures: \n"+bulkResponse.buildFailureMessage());
				}
			}
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
			
		
		
		// Delete an alert.
		//service.deleteAlert("@69d9c55");
		
		//System.out.println("---alert---"+alert);
	}

}
