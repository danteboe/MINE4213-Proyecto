SET use_cached_result = false;

WITH entradas AS (
  SELECT regexp_extract(path, '[^/]+$', 0) AS nombre
  FROM read_files(:ruta_entrada, format => 'binaryFile',
                  schema => 'path STRING, modificationTime TIMESTAMP, length BIGINT, content BINARY')
  WHERE path RLIKE '[.](csv[.]zst|csv|zip)$'
    AND endswith(regexp_replace(path, '[^/]+$', ''), :ruta_entrada)
),
confirmadas AS (
  SELECT regexp_extract(path, '[^/]+$', 0) AS nombre
  FROM read_files(:ruta_confirmados, format => 'binaryFile',
                  schema => 'path STRING, modificationTime TIMESTAMP, length BIGINT, content BINARY')
)
SELECT count(*) AS pendientes
FROM entradas
LEFT ANTI JOIN confirmadas USING (nombre)
