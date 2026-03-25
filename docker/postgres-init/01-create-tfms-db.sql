SELECT 'CREATE DATABASE tfms'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'tfms'
)\gexec
