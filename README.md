# orange-reporting-microservice (Spring Boot)

### This rest service is developed to generate three main reports for Mobile money API traffic.

- Totaol traffic report
- Operator Settlement report
- Service provider Settlement report


## Steps to build the jar file and run the service

clone the git repository to your local server
    
    link - https://github.com/DanukaPraneeth/orange-reporting-microservice.git
   
Go to the local repository, change "**application.properties**" if required to change the default custom configurations and execute the below command to build the pack

``` 
mvn clean install
```

Then start the service using below command (Java 8 is a product pre-requisit to run this service)

```
java -jar target/reports-0.0.1-SNAPSHOT.jar
```

## How to use the API service


Execute the below sample curl commands to generate these reports


```
curl -X GET 'http://localhost:8080/generatecsv/status-report?upper={starting-date, eg :2019-06-31 }&lower={ending-date, eg :2019-05-31 }&reportName=My-traffic-report'
```

```
curl -X GET 'http://localhost:8080/generatecsv/operator-report?upper={starting-date, eg :2019-06-31 }&lower={ending-date, eg :2019-05-31 }&reportName=My-operator-report'
```

```
curl -X GET 'http://localhost:8080/generatecsv/sp-report?upper={starting-date, eg :2019-06-31 }&lower={ending-date, eg :2019-05-31 }&reportName=My-sp-report'
```




