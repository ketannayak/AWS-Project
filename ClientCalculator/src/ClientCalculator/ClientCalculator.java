package ClientCalculator;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;



public class ClientCalculator {
	
	//Create the sqs client to which we'll send messages
	static AmazonSQSClient sqs; 
	
	
	
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
        sqs = new AmazonSQSClient(credentials);
        
	}

     //Private function that gets the list of integers
       private static String getnumbers() {
    	   
    	   String numbers = new String();
    	     	   
    	   try{
    		   
    		   System.out.println("Enter the list of integers:");
        	   Scanner scan  =new Scanner(System.in);
    		   numbers = scan.next();   
    		      		       		   
    	   }catch (Exception e){
    		
    		   System.out.println("An error occurred. Please make sure you entered the right inputs. Please enter only integers");
    		
    	   }
		return numbers;
    	   
       }
       
       
       private static String sendnumberstring() throws Exception{
    	   
    	    init();
  	   	 	sqs.setEndpoint("sqs.us-west-2.amazonaws.com");
  	   	 	
  	   	 	String clientsessionidtoken = new String();

  	        System.out.println("===========================================");
  	        System.out.println("Sending the number string to the Inbound Queue");
  	        System.out.println("===========================================\n");

  	        try {
  	        	
  	        	String inputnumbers = getnumbers();
  	        	System.out.println(inputnumbers);
  	            
  	        	// Get the details of the in-bound queue
  	            System.out.println("Getting the details of the inbound queue.\n");
  	            GetQueueUrlResult myQueueUrl = sqs.getQueueUrl("Inbound-SQS");
  	            System.out.println(myQueueUrl.getQueueUrl());
  	            
  	            UUID clientsessionid = UUID.randomUUID();
  	            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
  	            messageAttributes.put("SendRequestGUID", new MessageAttributeValue().withDataType("String").withStringValue(clientsessionid.toString()));

  	            // Send a message that is the string of integers that the user entered
  	            System.out.println("Sending a message to the location :   "+myQueueUrl.getQueueUrl() +" : \n"+ inputnumbers);
  	            SendMessageRequest request = new SendMessageRequest();
  	            request.withMessageBody(inputnumbers);
  	            request.withQueueUrl(myQueueUrl.getQueueUrl());
  	            request.withMessageAttributes(messageAttributes);
  	            
  	            sqs.sendMessage(request);
  	            System.out.println("Sent Message ID token:   "+ clientsessionid.toString());
  	            clientsessionidtoken = clientsessionid.toString();
  	             	            
  	            
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
  	        
  	        return clientsessionidtoken;
  	
     }   
    	   
      
         
      public static void main(String[] args) throws Exception {

    	   	init();
    	   	sqs.setEndpoint("sqs.us-west-2.amazonaws.com");
    	   	//Get the user input on the numbers and send the string to the in-bound SQS queue
    	   	String clientsessionid = sendnumberstring();
    	   	
            try {
            	
            	String result = new String();
            	GetQueueUrlResult outboundQueueUrl = sqs.getQueueUrl("Outbound-SQS");
                        	   	           	   	
        	    //Get number of messages in the out-bound queue
            	
        	   	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withWaitTimeSeconds(20);
        	   	receiveMessageRequest.withQueueUrl(outboundQueueUrl.getQueueUrl());
        	   	receiveMessageRequest.withMaxNumberOfMessages(10);
        	   	receiveMessageRequest.withMessageAttributeNames("SendRequestGUID");
                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                
              
                System.out.println(messages.size());
                
                for (Message message : messages) {
                	
                	System.out.println(message);
        	
       	                	
                	if( message.getMessageAttributes().containsKey("SendRequestGUID") && message.getMessageAttributes().get("SendRequestGUID").getStringValue().equals(clientsessionid)){
                		                		
                		//There is a message with the matched token
                		System.out.println("The server has returned an answer!");
                		     		
                		result =  message.getBody();
 
                			for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                				System.out.println("  Attribute");
                				System.out.println("    Name:  " + entry.getKey());
                				System.out.println("    Value: " + entry.getValue());
                			}
                			
                		// Delete a message
                        System.out.println("Cleaning up results.\n");
                        String messageRecieptHandle = message.getReceiptHandle();
                        sqs.deleteMessage(new DeleteMessageRequest(outboundQueueUrl.getQueueUrl(), messageRecieptHandle));	
	
                		break;
                	}
                	
                }
                           
                     
                System.out.println("Here are the results : ");
				System.out.println("After math operations :  " + result);
                

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

       
       
       
       
       
       
        
        
        
        
  
