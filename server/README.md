## Micronaut 4.5.0 Documentation

- [User Guide](https://docs.micronaut.io/4.5.0/guide/index.html)
- [API Reference](https://docs.micronaut.io/4.5.0/api/index.html)
- [Configuration Reference](https://docs.micronaut.io/4.5.0/guide/configurationreference.html)
- [Micronaut Guides](https://guides.micronaut.io/index.html)
---

- [Micronaut Maven Plugin documentation](https://micronaut-projects.github.io/micronaut-maven-plugin/latest/)
## Feature serialization-jackson documentation

- [Micronaut Serialization Jackson Core documentation](https://micronaut-projects.github.io/micronaut-serialization/latest/guide/)


## Feature maven-enforcer-plugin documentation

- [https://maven.apache.org/enforcer/maven-enforcer-plugin/](https://maven.apache.org/enforcer/maven-enforcer-plugin/)


## Feature micronaut-aot documentation

- [Micronaut AOT documentation](https://micronaut-projects.github.io/micronaut-aot/latest/guide/)



Testing

Start Minio Docker
```docker run    -p 9000:9000    -p 9090:9090    --name minio2    quay.io/minio/minio:latest server /data --console-address ":9090"
```

Upload test Data to s3 
 Run ManualTestUtil.java file inside test dir
Test with Series
```curl -v --output s.arrow -H "X-QueryId: 123" http://localhost:8080/v1/q/series?size=100```

Test with File
```curl -v --output count.arrow -H "Content-Type: application/json" \
--request POST \
--data '{"projections":["*"],"predicates":null,"groupBys":null,"sources":["s3://test-bucket/db/table/large_test.snappy.parquet"],"sortBys":null,"parameters":{"s3_access_key_id":"minioadmin","s3_use_ssl":"false","s3_secret_access_key":"minioadmin","s3_endpoint":"localhost:9000"},"limit":null}' \
http://localhost:8080/v1/q/parquet 
```


Dev environment in windows 
  - set env variable 
    ```Set-Item Env:JAVA_HOME  C:\Users\amayt\.jdks\graalvm-jdk-21.0.4```
  - run tests
    ```.\mvnw test -DskipTests```

Docker image 
  - ```../mvnw package -Dpackaging=docker -DskipTests```
Docker native image

Run the application 
  - ``` ./mvnw mn:run -Dmn.jvmArgs=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED```

Run the application to generate config for native image
    -   ```./mvnw mn:run -Dmn.jvmArgs="-agentlib:native-image-agent=config-output-dir=config-dir --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"```

Create native docker image 
    - ```./mvnw clean package -Dpackaging=docker-native -Dmicronaut.aot.enabled=true -Pgraalvm -DskipTests```