--
-- PostgreSQL database dump
--

-- Dumped from database version 15.1 (Debian 15.1-1.pgdg110+1)
-- Dumped by pg_dump version 15.1 (Debian 15.1-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: document_index; Type: TABLE; Schema: public; Owner: indexer
--

CREATE TABLE public.document_index (
    name text NOT NULL,
    set_name text NOT NULL,
    content_type text NOT NULL,
    mappings jsonb NOT NULL
);


ALTER TABLE public.document_index OWNER TO indexer;

--
-- Name: index_set; Type: TABLE; Schema: public; Owner: indexer
--

CREATE TABLE public.index_set (
    name text NOT NULL,
    "position" bigint NOT NULL
);


ALTER TABLE public.index_set OWNER TO indexer;

--
-- Name: indexing_override; Type: TABLE; Schema: public; Owner: indexer
--

CREATE TABLE public.indexing_override (
    content_type text NOT NULL,
    field text NOT NULL,
    mapping jsonb NOT NULL
);


ALTER TABLE public.indexing_override OWNER TO indexer;

--
-- Name: job_lock; Type: TABLE; Schema: public; Owner: indexer
--

CREATE TABLE public.job_lock (
    name text NOT NULL,
    holder text NOT NULL,
    touched timestamp with time zone NOT NULL,
    iteration bigint NOT NULL
);


ALTER TABLE public.job_lock OWNER TO indexer;

--
-- Name: schema_version; Type: TABLE; Schema: public; Owner: indexer
--

CREATE TABLE public.schema_version (
    version integer NOT NULL
);


ALTER TABLE public.schema_version OWNER TO indexer;

--
-- Name: document_index document_index_pkey; Type: CONSTRAINT; Schema: public; Owner: indexer
--

ALTER TABLE ONLY public.document_index
    ADD CONSTRAINT document_index_pkey PRIMARY KEY (name);


--
-- Name: index_set index_set_pkey; Type: CONSTRAINT; Schema: public; Owner: indexer
--

ALTER TABLE ONLY public.index_set
    ADD CONSTRAINT index_set_pkey PRIMARY KEY (name);


--
-- Name: indexing_override indexing_override_pkey; Type: CONSTRAINT; Schema: public; Owner: indexer
--

ALTER TABLE ONLY public.indexing_override
    ADD CONSTRAINT indexing_override_pkey PRIMARY KEY (content_type, field);


--
-- Name: job_lock job_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: indexer
--

ALTER TABLE ONLY public.job_lock
    ADD CONSTRAINT job_lock_pkey PRIMARY KEY (name);


--
-- Name: document_index_content_type_idx; Type: INDEX; Schema: public; Owner: indexer
--

CREATE INDEX document_index_content_type_idx ON public.document_index USING btree (content_type);


--
-- Name: document_index document_index_set_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: indexer
--

ALTER TABLE ONLY public.document_index
    ADD CONSTRAINT document_index_set_name_fkey FOREIGN KEY (set_name) REFERENCES public.index_set(name) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

