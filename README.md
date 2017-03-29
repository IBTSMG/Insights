# Insights
Sample Apache Spark jobs to analyze the transactions taking place in a core banking application with SOA

Analyzes the service oriented transaction data residing in HDFS. Each line of the input text files are expected to be json serialized, of the form below:

```json
{
"header": { 
    "aygKodu": "      ", 
      "transactionState": 9, 
      "entranceChannelCode": "999", 
      "jvmID": "999", 
      "pageInstanceId": "", 
      "channelTrxName": "IssueCreditTransaction", 
      "processTime": "075957", 
      "processState": 9, 
      "channelHostProcessCode": "0", 
      "processDate": "20170328", 
      "correlationId": "3e03bb4f-d175-44bc-904f-072b08116d4e", 
      "channelCode": "999", 
      "userCode": "XXXXXXXXX", 
      "transactionID": 99999999999999999999999999, 
      "processType": 0, 
      "environment": "XXXX", 
      "sessionId": "99999999999999999999999999", 
      "clientIp": "999.999.999.999" 
   }, 
   "services": [ 
	  { 
         "returnCode": 0, 
         "channelId": "999", 
         "parent": 0, 
         "poms": [], 
         "endTime": 1490677197467, 
         "platformId": "9", 
         "serviceName": "CREDIT_ISSUANCE_SERVICE", 
         "startTime": 1490677197466, 
         "level": 1, 
         "environment": "XXXX", 
         "order": 1, 
         "additionalInfos": {}, 
         "queries": [], 
         "referenceData": "CREDIT_ISSUANCE_OPERATION_PARAMETERS" 
      }, 
	  (...), 
	  { 
         "returnCode": 0, 
         "channelId": "999", 
         "parent": 5, 
         "poms": [], 
         "endTime": 1490677197491, 
         "platformId": "9", 
         "serviceName": "GET_CUSTOMER_INFORMATION", 
         "startTime": 1490677197491, 
         "level": 6, 
         "environment": "XXXX", 
         "order": 18, 
         "additionalInfos": {}, 
         "queries": [ 
            { 
               "tables": "CUSTOMER_MAIN,CUSTOMER_EXT", 
               "startTime": 1490677197491, 
               "order": 1, 
               "queryName": "SELECT_CUSTOMER_DATA", 
               "isExecuted": true, 
               "parent": 18, 
               "type": 1, 
               "endTime": 1490677197491 
            } 
         ], 
         "referenceData": "" 
      }, 
      { 
         "returnCode": 0, 
         "channelId": "999", 
         "parent": 6, 
         "poms": [], 
         "endTime": 1490677197467, 
         "platformId": "9", 
         "serviceName": "GET_PRICING_POLICY", 
         "startTime": 1490677197466, 
         "level": 7, 
         "environment": "XXXX", 
         "order": 7, 
         "additionalInfos": {}, 
         "queries": [], 
         "referenceData": "" 
      }, 
      { 
         "returnCode": 0, 
         "channelId": "999", 
         "parent": 5, 
         "poms": [], 
         "endTime": 1490677197468, 
         "platformId": "9", 
         "serviceName": "CALCULATE_ISSUANCE_COMMISSIONS", 
         "startTime": 1490677197466, 
         "level": 6, 
         "environment": "XXXX", 
         "order": 6, 
         "additionalInfos": {}, 
         "queries": [], 
         "referenceData": "" 
      }, 
      (...), 
      { 
         "returnCode": 0, 
         "channelId": "999", 
         "parent": 18, 
         "poms": [], 
         "endTime": 1490677197491, 
         "platformId": "9", 
         "serviceName": "CREDIT_ISSUANCE_DOCUMENT_SERVICE", 
         "startTime": 1490677197491, 
         "level": 9, 
         "environment": "XXXX", 
         "order": 19, 
         "additionalInfos": {}, 
         "queries": [ 
            { 
               "tables": "ISSUANCE_MAIN,CUSTOMER_MAIN", 
               "startTime": 1490677197491, 
               "order": 1, 
               "queryName": "SELECT_CUSTOMER_ISSUANCE_INFORMATION", 
               "isExecuted": true, 
               "parent": 19, 
               "type": 1, 
               "endTime": 1490677197491 
            } 
         ], 
         "referenceData": "" 
      }, 
      (...) 
   ] 
}
```

Four separate analysis jobs are present in the src/com/ibtsmg/insights/jobs/ package, all of which post the outputs to an ElasticSearch cluster, as well as persisting to Hive tables.

Examples of Kibana visualizations for the jobs:

<b>DependencyTreeAnalysis.java (rightmost), TransactionDetailAnalysis.java (leftmost & middle):</b>
<img src="https://raw.githubusercontent.com/IBTSMG/Insights/master/screenshots/issuance_dependency_dashboard.jpg"/>

<b>ServiceCallTreeAnalysis.java:</b>
<img src="https://raw.githubusercontent.com/IBTSMG/Insights/master/screenshots/service_call_tree.png"/>

<b>ServiceDurationDetails.java:</b>
<img src="https://raw.githubusercontent.com/IBTSMG/Insights/master/screenshots/issuance_service_dashboard.jpg"/>




