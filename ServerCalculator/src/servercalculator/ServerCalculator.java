package servercalculator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;



public class ServerCalculator {
	
	//Create the sqs client to which we'll send messages
		static AmazonSQSClient sqsout;
		static AmazonSQSClient sqsin; 
	//Create the S3 resource to which we'll log requests
		static AmazonS3Client s3;
				
		
		//Private function that validates the user
		private static void init() throws Exception {

	        /*
	         * The ProfileCredentialsProvider will return the [default]
	         * credential profile by reading from the credentials file located at
	         * (C:\\Users\\Ketan\\.aws\\credentials).
	         */
	        AWSCredentials credentials = null;
	        try {
	            credentials = new ProfileCredentialsProvider("default").getCredentials();
	        } catch (Exception e) {
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location and is in valid format.", e);
	        }
	        sqsin = new AmazonSQSClient(credentials);
	        sqsout = new AmazonSQSClient(credentials);
	        s3 = new AmazonS3Client(credentials);
	        
		}

       
	       private static SendMessageResult sendresultstring(String clientsessionid, String resultstring) throws Exception{
	    	   
	    	   //Send message to out-bound queue
	  	   	 	sqsout.setEndpoint("sqs.us-west-2.amazonaws.com");
	  	   	 	SendMessageResult sentmessageresult = new SendMessageResult();
	  	   	 	
	  	   	 	try {
	  	        	
	  	        	// Get the details of the in-bound queue
	  	            GetQueueUrlResult myQueueUrl = sqsout.getQueueUrl("Outbound-SQS");
	  	            
	  	            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
	  	            messageAttributes.put("SendRequestGUID", new MessageAttributeValue().withDataType("String").withStringValue(clientsessionid));

	  	            // Send a message that is the string of integers that the user entered
	  	            SendMessageRequest request = new SendMessageRequest();
	  	            request.withMessageBody(resultstring);
	  	            request.withQueueUrl(myQueueUrl.getQueueUrl());
	  	            request.withMessageAttributes(messageAttributes);
	  	            sentmessageresult= sqsout.sendMessage(request);
	  	            
	  	           
	  	             	            
	  	            
	  	        } catch (AmazonServiceException ase) {
	  	            System.out.println("Caught an AmazonServiceException, which means your request made it " +
	  	                    "to Amazon SQS, but was rejected with an error response for some reason.");
	  	            System.out.println("Error Message:    " + ase.getMessage());
	  	            System.out.println("HTTP Status Code: " + ase.getStatusCode());
	  	            System.out.println("AWS Error Code:   " + ase.getErrorCode());
	  	            System.out.println("Error Type:       " + ase.getErrorType());
	  	            System.out.println("Request ID:       " + ase.getRequestId());
	  	        } catch (AmazonClientException ace) {
	  	            System.out.println("Caught an AmazonClientException, which means the client encountered " +
	  	                    "a serious internal problem while trying to communicate with SQS, such as not " +
	  	                    "being able to access the network.");
	  	            System.out.println("Error Message: " + ace.getMessage());
	  	        }
	  	        
	  	        return sentmessageresult;
	  	
	     }   
	       
	       
	      private static Message readmessage() throws Exception{
	    	  
	    	sqsin.setEndpoint("sqs.us-west-2.amazonaws.com");  
 	    	Message receivedmessage = new Message();
	    	      
            try {
            	
    	    	
              	GetQueueUrlResult inboundQueueUrl = sqsin.getQueueUrl("Inbound-SQS");
                          	   	           	   	
          	    //Fetch a message from the in-bound queue
              	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withWaitTimeSeconds(20);
          	   	receiveMessageRequest.withQueueUrl(inboundQueueUrl.getQueueUrl());
          	   	receiveMessageRequest.withMaxNumberOfMessages(1);
          	   	receiveMessageRequest.withMessageAttributeNames("SendRequestGUID");
                List<Message> messages = sqsin.receiveMessage(receiveMessageRequest).getMessages();
                               
              
                if(messages.size()>0){
                
                	receivedmessage = messages.get(0);
                	
                	//Deleting the message from the inbox
                	// Delete a message
                    System.out.println("Cleaning up results.\n");
                    String messageRecieptHandle = receivedmessage.getReceiptHandle();
                    sqsin.deleteMessage(new DeleteMessageRequest(inboundQueueUrl.getQueueUrl(), messageRecieptHandle));             
     
                }
                else
                {                	
                 System.out.println("There are no more messages to process!!");	
                }
                
                
                
                
                
             } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which means your request made it " +
                        "to Amazon SQS, but was rejected with an error response for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which means the client encountered " +
                        "a serious internal problem while trying to communicate with SQS, such as not " +
                        "being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());
            }
            
            return receivedmessage;
            
            
      }
                         
            	      
       private static File createSampleFile(Message receivedmessage, SendMessageResult sentmessage) throws IOException {
	    	  
	    	  String clientsessionid  = receivedmessage.getMessageAttributes().get("SendRequestGUID").getStringValue();
	    	  
	          File file = File.createTempFile("aws-java-sdk-"+clientsessionid, ".txt");
	          file.deleteOnExit();

	          Writer writer = new OutputStreamWriter(new FileOutputStream(file));
	          writer.write("Received Message \n");
	          writer.write("ClientSessionId :"+clientsessionid +"\n");
	          writer.write(receivedmessage.toString()+"\n");
	          writer.write("Input string received:"+receivedmessage.getBody()+"\n");
	          writer.write("-------------------- \n");
	          writer.write("Sent Message \n");
	          writer.write(sentmessage.toString()+"\n");
	          writer.close();

	          return file;
	      }
	      
	      
	      
	      private static String mathcalculator(String input) throws Exception{
	    	  
	    	  String result =new String();
	    	  Map<String, Integer> resultmap = new HashMap<String, Integer>();
	    	  Integer min=0, max=0,sum=0,product=1,sumofsquares=0;
	    	  
	    	  try {
	    		  
	    		  List<String> listofnumberstrings = Arrays.asList(input.split("\\s*,\\s*")); 
	    		  ArrayList<Integer> listofnumbers = new ArrayList<Integer>();
	    		  for(String s: listofnumberstrings) listofnumbers.add(Integer.valueOf(s));
	    		
	    		  min = Collections.min(listofnumbers);
	    		  max = Collections.max(listofnumbers);
	    		      		 
	    		  
	    		  for (Integer i : listofnumbers){
	    			  
	    			  sum+=i;
	    			  product*=i;
	    			  sumofsquares+=i*i;
  
	    		  }
	            	
	    		  resultmap.put("Min", min);
	    		  resultmap.put("Max", max);
	    		  resultmap.put("Product", product);
	    		  resultmap.put("Sum", sum);
	    		  resultmap.put("Sum-of-Squares", sumofsquares);
	    		  
	    		  result = resultmap.toString();
           

	            } catch (AmazonServiceException ase) {
	                System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                        "to Amazon SQS, but was rejected with an error response for some reason.");
	                System.out.println("Error Message:    " + ase.getMessage());
	                System.out.println("HTTP Status Code: " + ase.getStatusCode());
	                System.out.println("AWS Error Code:   " + ase.getErrorCode());
	                System.out.println("Error Type:       " + ase.getErrorType());
	                System.out.println("Request ID:       " + ase.getRequestId());
	            } catch (AmazonClientException ace) {
	                System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                        "a serious internal problem while trying to communicate with SQS, such as not " +
	                        "being able to access the network.");
	                System.out.println("Error Message: " + ace.getMessage());
	            }
	    	   	
	    	  return result;
	      }
	    	   
	      
	         
	      public static void main(String[] args) throws Exception {

	    	   	init();
	            Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	            s3.setRegion(usWest2);
		    	     	   		    	   	

	            try {
	            	
	            	//Get the current time-stamp
	            	Long timestart = System.currentTimeMillis();

	            	// Keep polling
	            	while (System.currentTimeMillis()<=(timestart+300000)){
	            	
	            		//Read the in-bound queue and fetch a message
	            		Message receivedmessage = readmessage();
	            			            		
	            		if(receivedmessage.getMessageAttributes().containsKey("SendRequestGUID")){
	            			//Calculate the result
	            			String result = mathcalculator(receivedmessage.getBody());   	
	            			System.out.println(result);
	            			//Send the message
	            			String clientsessionid = receivedmessage.getMessageAttributes().get("SendRequestGUID").getStringValue();
	            			SendMessageResult sentmessageresult = sendresultstring(clientsessionid,result); 
	            			System.out.println(sentmessageresult.toString());
  	   	
	            			s3.putObject(new PutObjectRequest(s3.listBuckets().get(0).getName()+"/ArithmeticCalculatorLogs", clientsessionid,createSampleFile(receivedmessage,sentmessageresult)));
	            			
	            		}
	            	}


	            } catch (AmazonServiceException ase) {
	                System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                        "to Amazon SQS, but was rejected with an error response for some reason.");
	                System.out.println("Error Message:    " + ase.getMessage());
	                System.out.println("HTTP Status Code: " + ase.getStatusCode());
	                System.out.println("AWS Error Code:   " + ase.getErrorCode());
	                System.out.println("Error Type:       " + ase.getErrorType());
	                System.out.println("Request ID:       " + ase.getRequestId());
	            } catch (AmazonClientException ace) {
	                System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                        "a serious internal problem while trying to communicate with SQS, such as not " +
	                        "being able to access the network.");
	                System.out.println("Error Message: " + ace.getMessage());
	            }
	    	   	
	    	   
	    	   
	    	   
	      }

}
