--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1 (Debian 16.1-1.pgdg120+1)
-- Dumped by pg_dump version 16.1 (Debian 16.1-1.pgdg120+1)

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
-- Name: cluster; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cluster (
    name text NOT NULL,
    url text NOT NULL,
    auth jsonb NOT NULL,
    created timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: document_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document_index (
    name text NOT NULL,
    set_name text NOT NULL,
    content_type text NOT NULL,
    mappings jsonb NOT NULL
);


--
-- Name: index_set; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.index_set (
    name text NOT NULL,
    "position" bigint NOT NULL,
    cluster text,
    active boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    modified timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: indexing_override; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.indexing_override (
    content_type text NOT NULL,
    field text NOT NULL,
    mapping jsonb NOT NULL
);


--
-- Name: job_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_lock (
    name text NOT NULL,
    holder text NOT NULL,
    touched timestamp with time zone NOT NULL,
    iteration bigint NOT NULL
);


--
-- Name: schema_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_version (
    version integer NOT NULL
);


--
-- Name: cluster cluster_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cluster
    ADD CONSTRAINT cluster_pkey PRIMARY KEY (name);


--
-- Name: document_index document_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_index
    ADD CONSTRAINT document_index_pkey PRIMARY KEY (name);


--
-- Name: index_set index_set_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.index_set
    ADD CONSTRAINT index_set_pkey PRIMARY KEY (name);


--
-- Name: indexing_override indexing_override_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.indexing_override
    ADD CONSTRAINT indexing_override_pkey PRIMARY KEY (content_type, field);


--
-- Name: job_lock job_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_lock
    ADD CONSTRAINT job_lock_pkey PRIMARY KEY (name);


--
-- Name: document_index_content_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX document_index_content_type_idx ON public.document_index USING btree (content_type);


--
-- Name: unique_single_active; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX unique_single_active ON public.index_set USING btree (active) WHERE (active = true);


--
-- Name: document_index document_index_set_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_index
    ADD CONSTRAINT document_index_set_name_fkey FOREIGN KEY (set_name) REFERENCES public.index_set(name) ON DELETE CASCADE;


--
-- Name: index_set fk_set_cluster; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.index_set
    ADD CONSTRAINT fk_set_cluster FOREIGN KEY (cluster) REFERENCES public.cluster(name) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

