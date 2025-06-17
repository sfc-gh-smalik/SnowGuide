DROP SERVICE IF EXISTS snowguide_web_crawler_service;

EXECUTE JOB SERVICE
  IN COMPUTE POOL snowguide_compute_pool
  NAME = snowguide_web_crawler_service
  FROM SPECIFICATION $$
spec:
  containers:
  - name: main
    image: /openai/snowguide/snowguide_repository/snowguide_webcrawler:latest
    volumeMounts:
    - name: data
      mountPath: /usr/data
  volumes:
  - name: data
    source: "@data"
$$;