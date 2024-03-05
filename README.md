## Сборка и запуск
1. Для сборки jar нужно выполнить sbt clean package
2. Директорию bostoncrimes положить рядом со spark-submit
3. Для запуска выполнить spark-submit ${PROJECT_DIR}/target/scala-2.12/boston-crimes_2.12-0.1.0-SNAPSHOT.jar 
