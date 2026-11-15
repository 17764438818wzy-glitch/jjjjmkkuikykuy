CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
CREATE TABLE IF NOT EXISTS major (
 code text PRIMARY KEY, name text NOT NULL, paper_dataset_id text NOT NULL, image_dataset_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS article (
 id text PRIMARY KEY, source_key text UNIQUE NOT NULL, base jsonb NOT NULL,
 overrides jsonb NOT NULL DEFAULT '{}', revision integer NOT NULL DEFAULT 1,
 title text NOT NULL, folder_name text NOT NULL,
 sync jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS article_major (
 article_id text NOT NULL REFERENCES article(id), code text NOT NULL,
 PRIMARY KEY(article_id, code)
);
CREATE INDEX IF NOT EXISTS article_major_filter ON article_major(code, article_id);
CREATE INDEX IF NOT EXISTS article_order ON article(updated_at DESC,id);
CREATE INDEX IF NOT EXISTS article_research_type ON article((base#>>'{profile,research_profile,research_type}'));
CREATE INDEX IF NOT EXISTS article_year ON article((base#>>'{profile,article,year}'));
-- Derived query text follows existing profile fields and manual overrides automatically.
-- No profile or source identity is rewritten when this column is populated.
ALTER TABLE article ADD COLUMN IF NOT EXISTS search_text text GENERATED ALWAYS AS (
 coalesce(title,'') || ' ' ||
 coalesce(overrides#>>'{fields,summary}',nullif(base#>>'{profile,article,summary}',''),base#>>'{profile,summary}','') || ' ' ||
 coalesce(overrides#>>'{fields,keywords}',nullif(nullif(base#>>'{profile,article,keywords}',''),'[]'),base#>>'{profile,keywords}','') || ' ' ||
 coalesce(overrides#>>'{fields,methods}',jsonb_path_query_array(base,'$.profile.learning_units[*].method')::text,'') || ' ' ||
 coalesce(overrides#>>'{fields,learn}',
   coalesce(jsonb_path_query_array(base,'$.profile.learning_units[*].what_to_learn')::text,'') || ' ' ||
   coalesce(jsonb_path_query_array(base,'$.profile.learning_units[*].learn')::text,'') || ' ' ||
   coalesce(base#>>'{profile,migration_boundary,reusable_scope}','') || ' ' ||
   coalesce(base#>>'{profile,reusable_scope}',''), '')
) STORED;
CREATE TABLE IF NOT EXISTS source_status (
 name text PRIMARY KEY, status jsonb NOT NULL, observed_at timestamptz NOT NULL DEFAULT now()
);

