---Creacion del stream de entrada 

CREATE STREAM sales_stream 
WITH (
  KAFKA_TOPIC='sales-transactions', 
  VALUE_FORMAT='AVRO',
  KEY_FORMAT='KAFKA'
);


---creacion de la tabla sales_summary_ksql con logica de agregacion y ventana

CREATE TABLE sales_summary_ksql 
WITH (KAFKA_TOPIC='sales-summary-ksql', VALUE_FORMAT='AVRO') AS
  SELECT 
    category,
    SUM(quantity) AS total_quantity,
    SUM(price) AS total_revenue,
    FROM_UNIXTIME(WINDOWSTART) AS window_start,
    FROM_UNIXTIME(WINDOWEND) AS window_end
  FROM sales_stream
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY category;
  
  
  -----Verificacion de la recepcion de mensajes en la tabla sales_summary_ksql
  
  SELECT * FROM sales_summary_ksql EMIT CHANGES LIMIT 100;