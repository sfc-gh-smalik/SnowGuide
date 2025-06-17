DROP SERVICE IF EXISTS snowguide_content_loader;

EXECUTE JOB SERVICE
  IN COMPUTE POOL snowguide_compute_pool
  NAME = snowguide_content_loader
  FROM SPECIFICATION $$
spec:
  containers:
  - name: main
    image: /openai/snowguide/snowguide_repository/snowguide_content_loader:latest
    volumeMounts:
    - name: data
      mountPath: /usr/data
  volumes:
  - name: data
    source: "@data"
$$;