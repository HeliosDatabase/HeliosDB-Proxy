CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE docs (
    id        SERIAL PRIMARY KEY,
    title     TEXT NOT NULL,
    embedding vector(3) NOT NULL
);

INSERT INTO docs (title, embedding) VALUES
  ('intro',     '[1, 0, 0]'),
  ('chapter1',  '[0.9, 0.1, 0.0]'),
  ('chapter2',  '[0.8, 0.2, 0.0]'),
  ('chapter3',  '[0.0, 1.0, 0.0]'),
  ('chapter4',  '[0.0, 0.9, 0.1]'),
  ('chapter5',  '[0.0, 0.0, 1.0]');

CREATE INDEX ON docs USING hnsw (embedding vector_cosine_ops);
