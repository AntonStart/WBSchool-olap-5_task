# WBSchool-olap-5_task
SPARK

ЗАДАНИЕ

1. Считать данные из вашей Кафки через спарк. Если нужно, залейте немного данных с пегас.
2. Добавить какую-нибудь колонку. Записать в ваш клик в докере.
*Можно через csv импортировать в ваш клик справочник объемов nm_id с пегаса, чтобы оттуда брать объем номенклатуры.
3. Выложить папку с docker-compose файлами для развертывания контейнеров. Должно быть 2 файла: docker-compose.yml, .env.
4. Запушить в свой гит получившийся таск спарк. Не пушить файл с паролями.
5. Выложить в гит скрины с содержимым конечной папки в вашем клике. 
6. Выложить код структуру конечной таблицы в вашем клике.
7. Выложить скрин веб интерфейса вашего спарк.
8. Скрин работы вашего приложения из консоли контейнера.

РЕШЕНИЕ
1. Решение начинается с п.3
2. Решение начинается с п.3
3. docker-compose директории: [clickhouse](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/docker/docker_clickhouse/docker-compose.yml), [kafka](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/docker/docker_kafka/docker-compose.yml), [spark](https://github.com/AntonStart/WBSchool-olap-5_task/tree/main/docker/docker_spark)
4. [task Spark](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/spark_worker/tareload_edu/spark_pipeline.py)
5. [Скрин содержимого клика](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202024-07-18%20142109.png)
6. [Код, структура конечной таблицы в Clickhouse](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/DDL.sql)
7. [Скрин Веб интерфейса Spark](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202024-07-18%20142645.png)
8. [Скрин работы приложения из консоли](https://github.com/AntonStart/WBSchool-olap-5_task/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202024-07-18%20142548.png)
