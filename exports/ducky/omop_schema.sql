--
-- PostgreSQL database dump
--

\restrict 9KfiVTbB6sO0pS8kCdE8SivaG1TJpeWniCEpYAKeFGCR9blOGky98HIXeqciz78

-- Dumped from database version 15.16 (Debian 15.16-1.pgdg13+1)
-- Dumped by pg_dump version 15.16 (Debian 15.16-1.pgdg13+1)

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

--
-- Name: ohdsi; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ohdsi;


ALTER SCHEMA ohdsi OWNER TO postgres;

--
-- Name: webapi_results; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA webapi_results;


ALTER SCHEMA webapi_results OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: care_site; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.care_site (
    care_site_id integer NOT NULL,
    care_site_name character varying(255),
    place_of_service_concept_id integer,
    location_id integer,
    care_site_source_value character varying(50),
    place_of_service_source_value character varying(50)
);


ALTER TABLE public.care_site OWNER TO postgres;

--
-- Name: cdm_source; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cdm_source (
    cdm_source_name character varying(255) NOT NULL,
    cdm_source_abbreviation character varying(25) NOT NULL,
    cdm_holder character varying(255) NOT NULL,
    source_description text,
    source_documentation_reference character varying(255),
    cdm_etl_reference character varying(255),
    source_release_date date NOT NULL,
    cdm_release_date date NOT NULL,
    cdm_version character varying(10),
    cdm_version_concept_id integer NOT NULL,
    vocabulary_version character varying(20) NOT NULL
);


ALTER TABLE public.cdm_source OWNER TO postgres;

--
-- Name: cohort; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cohort (
    cohort_definition_id integer NOT NULL,
    subject_id integer NOT NULL,
    cohort_start_date date NOT NULL,
    cohort_end_date date NOT NULL
);


ALTER TABLE public.cohort OWNER TO postgres;

--
-- Name: cohort_definition; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cohort_definition (
    cohort_definition_id integer NOT NULL,
    cohort_definition_name character varying(255) NOT NULL,
    cohort_definition_description text,
    definition_type_concept_id integer NOT NULL,
    cohort_definition_syntax text,
    subject_concept_id integer NOT NULL,
    cohort_initiation_date date
);


ALTER TABLE public.cohort_definition OWNER TO postgres;

--
-- Name: concept; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.concept (
    concept_id integer NOT NULL,
    concept_name character varying(255) NOT NULL,
    domain_id character varying(20) NOT NULL,
    vocabulary_id character varying(20) NOT NULL,
    concept_class_id character varying(20) NOT NULL,
    standard_concept character varying(1),
    concept_code character varying(50) NOT NULL,
    valid_start_date date NOT NULL,
    valid_end_date date NOT NULL,
    invalid_reason character varying(1)
);


ALTER TABLE public.concept OWNER TO postgres;

--
-- Name: concept_ancestor; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.concept_ancestor (
    ancestor_concept_id integer NOT NULL,
    descendant_concept_id integer NOT NULL,
    min_levels_of_separation integer NOT NULL,
    max_levels_of_separation integer NOT NULL
);


ALTER TABLE public.concept_ancestor OWNER TO postgres;

--
-- Name: concept_class; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.concept_class (
    concept_class_id character varying(20) NOT NULL,
    concept_class_name character varying(255) NOT NULL,
    concept_class_concept_id integer NOT NULL
);


ALTER TABLE public.concept_class OWNER TO postgres;

--
-- Name: concept_relationship; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.concept_relationship (
    concept_id_1 integer NOT NULL,
    concept_id_2 integer NOT NULL,
    relationship_id character varying(20) NOT NULL,
    valid_start_date date NOT NULL,
    valid_end_date date NOT NULL,
    invalid_reason character varying(1)
);


ALTER TABLE public.concept_relationship OWNER TO postgres;

--
-- Name: concept_synonym; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.concept_synonym (
    concept_id integer NOT NULL,
    concept_synonym_name character varying(1000) NOT NULL,
    language_concept_id integer NOT NULL
);


ALTER TABLE public.concept_synonym OWNER TO postgres;

--
-- Name: condition_era; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.condition_era (
    condition_era_id integer NOT NULL,
    person_id integer NOT NULL,
    condition_concept_id integer NOT NULL,
    condition_era_start_date date NOT NULL,
    condition_era_end_date date NOT NULL,
    condition_occurrence_count integer
);


ALTER TABLE public.condition_era OWNER TO postgres;

--
-- Name: condition_occurrence; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.condition_occurrence (
    condition_occurrence_id integer NOT NULL,
    person_id integer NOT NULL,
    condition_concept_id integer NOT NULL,
    condition_start_date date NOT NULL,
    condition_start_datetime timestamp without time zone,
    condition_end_date date,
    condition_end_datetime timestamp without time zone,
    condition_type_concept_id integer NOT NULL,
    condition_status_concept_id integer,
    stop_reason character varying(20),
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    condition_source_value character varying(50),
    condition_source_concept_id integer,
    condition_status_source_value character varying(50)
);


ALTER TABLE public.condition_occurrence OWNER TO postgres;

--
-- Name: cost; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cost (
    cost_id integer NOT NULL,
    cost_event_id integer NOT NULL,
    cost_domain_id character varying(20) NOT NULL,
    cost_type_concept_id integer NOT NULL,
    currency_concept_id integer,
    total_charge numeric,
    total_cost numeric,
    total_paid numeric,
    paid_by_payer numeric,
    paid_by_patient numeric,
    paid_patient_copay numeric,
    paid_patient_coinsurance numeric,
    paid_patient_deductible numeric,
    paid_by_primary numeric,
    paid_ingredient_cost numeric,
    paid_dispensing_fee numeric,
    payer_plan_period_id integer,
    amount_allowed numeric,
    revenue_code_concept_id integer,
    revenue_code_source_value character varying(50),
    drg_concept_id integer,
    drg_source_value character varying(3)
);


ALTER TABLE public.cost OWNER TO postgres;

--
-- Name: death; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.death (
    person_id integer NOT NULL,
    death_date date NOT NULL,
    death_datetime timestamp without time zone,
    death_type_concept_id integer,
    cause_concept_id integer,
    cause_source_value character varying(50),
    cause_source_concept_id integer
);


ALTER TABLE public.death OWNER TO postgres;

--
-- Name: device_exposure; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.device_exposure (
    device_exposure_id integer NOT NULL,
    person_id integer NOT NULL,
    device_concept_id integer NOT NULL,
    device_exposure_start_date date NOT NULL,
    device_exposure_start_datetime timestamp without time zone,
    device_exposure_end_date date,
    device_exposure_end_datetime timestamp without time zone,
    device_type_concept_id integer NOT NULL,
    unique_device_id character varying(255),
    production_id character varying(255),
    quantity integer,
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    device_source_value character varying(50),
    device_source_concept_id integer,
    unit_concept_id integer,
    unit_source_value character varying(50),
    unit_source_concept_id integer
);


ALTER TABLE public.device_exposure OWNER TO postgres;

--
-- Name: domain; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.domain (
    domain_id character varying(20) NOT NULL,
    domain_name character varying(255) NOT NULL,
    domain_concept_id integer NOT NULL
);


ALTER TABLE public.domain OWNER TO postgres;

--
-- Name: dose_era; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dose_era (
    dose_era_id integer NOT NULL,
    person_id integer NOT NULL,
    drug_concept_id integer NOT NULL,
    unit_concept_id integer NOT NULL,
    dose_value numeric NOT NULL,
    dose_era_start_date date NOT NULL,
    dose_era_end_date date NOT NULL
);


ALTER TABLE public.dose_era OWNER TO postgres;

--
-- Name: drug_era; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.drug_era (
    drug_era_id integer NOT NULL,
    person_id integer NOT NULL,
    drug_concept_id integer NOT NULL,
    drug_era_start_date date NOT NULL,
    drug_era_end_date date NOT NULL,
    drug_exposure_count integer,
    gap_days integer
);


ALTER TABLE public.drug_era OWNER TO postgres;

--
-- Name: drug_exposure; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.drug_exposure (
    drug_exposure_id integer NOT NULL,
    person_id integer NOT NULL,
    drug_concept_id integer NOT NULL,
    drug_exposure_start_date date NOT NULL,
    drug_exposure_start_datetime timestamp without time zone,
    drug_exposure_end_date date NOT NULL,
    drug_exposure_end_datetime timestamp without time zone,
    verbatim_end_date date,
    drug_type_concept_id integer NOT NULL,
    stop_reason character varying(20),
    refills integer,
    quantity numeric,
    days_supply integer,
    sig text,
    route_concept_id integer,
    lot_number character varying(50),
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    drug_source_value character varying(50),
    drug_source_concept_id integer,
    route_source_value character varying(50),
    dose_unit_source_value character varying(50)
);


ALTER TABLE public.drug_exposure OWNER TO postgres;

--
-- Name: drug_strength; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.drug_strength (
    drug_concept_id integer NOT NULL,
    ingredient_concept_id integer NOT NULL,
    amount_value numeric,
    amount_unit_concept_id integer,
    numerator_value numeric,
    numerator_unit_concept_id integer,
    denominator_value numeric,
    denominator_unit_concept_id integer,
    box_size integer,
    valid_start_date date NOT NULL,
    valid_end_date date NOT NULL,
    invalid_reason character varying(1)
);


ALTER TABLE public.drug_strength OWNER TO postgres;

--
-- Name: episode; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.episode (
    episode_id integer NOT NULL,
    person_id integer NOT NULL,
    episode_concept_id integer NOT NULL,
    episode_start_date date NOT NULL,
    episode_start_datetime timestamp without time zone,
    episode_end_date date,
    episode_end_datetime timestamp without time zone,
    episode_parent_id integer,
    episode_number integer,
    episode_object_concept_id integer NOT NULL,
    episode_type_concept_id integer NOT NULL,
    episode_source_value character varying(50),
    episode_source_concept_id integer
);


ALTER TABLE public.episode OWNER TO postgres;

--
-- Name: episode_event; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.episode_event (
    episode_id integer NOT NULL,
    event_id integer NOT NULL,
    episode_event_field_concept_id integer NOT NULL
);


ALTER TABLE public.episode_event OWNER TO postgres;

--
-- Name: fact_relationship; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_relationship (
    domain_concept_id_1 integer NOT NULL,
    fact_id_1 integer NOT NULL,
    domain_concept_id_2 integer NOT NULL,
    fact_id_2 integer NOT NULL,
    relationship_concept_id integer NOT NULL
);


ALTER TABLE public.fact_relationship OWNER TO postgres;

--
-- Name: location; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.location (
    location_id integer NOT NULL,
    address_1 character varying(50),
    address_2 character varying(50),
    city character varying(50),
    state character varying(2),
    zip character varying(9),
    county character varying(20),
    location_source_value character varying(50),
    country_concept_id integer,
    country_source_value character varying(80),
    latitude numeric,
    longitude numeric
);


ALTER TABLE public.location OWNER TO postgres;

--
-- Name: measurement; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.measurement (
    measurement_id integer NOT NULL,
    person_id integer NOT NULL,
    measurement_concept_id integer NOT NULL,
    measurement_date date NOT NULL,
    measurement_datetime timestamp without time zone,
    measurement_time character varying(10),
    measurement_type_concept_id integer NOT NULL,
    operator_concept_id integer,
    value_as_number numeric,
    value_as_concept_id integer,
    unit_concept_id integer,
    range_low numeric,
    range_high numeric,
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    measurement_source_value character varying(50),
    measurement_source_concept_id integer,
    unit_source_value character varying(50),
    unit_source_concept_id integer,
    value_source_value character varying(50),
    measurement_event_id integer,
    meas_event_field_concept_id integer
);


ALTER TABLE public.measurement OWNER TO postgres;

--
-- Name: metadata; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.metadata (
    metadata_id integer NOT NULL,
    metadata_concept_id integer NOT NULL,
    metadata_type_concept_id integer NOT NULL,
    name character varying(250) NOT NULL,
    value_as_string character varying(250),
    value_as_concept_id integer,
    value_as_number numeric,
    metadata_date date,
    metadata_datetime timestamp without time zone
);


ALTER TABLE public.metadata OWNER TO postgres;

--
-- Name: note; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.note (
    note_id integer NOT NULL,
    person_id integer NOT NULL,
    note_date date NOT NULL,
    note_datetime timestamp without time zone,
    note_type_concept_id integer NOT NULL,
    note_class_concept_id integer NOT NULL,
    note_title character varying(250),
    note_text text NOT NULL,
    encoding_concept_id integer NOT NULL,
    language_concept_id integer NOT NULL,
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    note_source_value character varying(50),
    note_event_id integer,
    note_event_field_concept_id integer
);


ALTER TABLE public.note OWNER TO postgres;

--
-- Name: note_nlp; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.note_nlp (
    note_nlp_id integer NOT NULL,
    note_id integer NOT NULL,
    section_concept_id integer,
    snippet character varying(250),
    "offset" character varying(50),
    lexical_variant character varying(250) NOT NULL,
    note_nlp_concept_id integer,
    note_nlp_source_concept_id integer,
    nlp_system character varying(250),
    nlp_date date NOT NULL,
    nlp_datetime timestamp without time zone,
    term_exists character varying(1),
    term_temporal character varying(50),
    term_modifiers character varying(2000)
);


ALTER TABLE public.note_nlp OWNER TO postgres;

--
-- Name: observation; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.observation (
    observation_id integer NOT NULL,
    person_id integer NOT NULL,
    observation_concept_id integer NOT NULL,
    observation_date date NOT NULL,
    observation_datetime timestamp without time zone,
    observation_type_concept_id integer NOT NULL,
    value_as_number numeric,
    value_as_string character varying(60),
    value_as_concept_id integer,
    qualifier_concept_id integer,
    unit_concept_id integer,
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    observation_source_value character varying(50),
    observation_source_concept_id integer,
    unit_source_value character varying(50),
    qualifier_source_value character varying(50),
    value_source_value character varying(50),
    observation_event_id integer,
    obs_event_field_concept_id integer
);


ALTER TABLE public.observation OWNER TO postgres;

--
-- Name: observation_period; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.observation_period (
    observation_period_id integer NOT NULL,
    person_id integer NOT NULL,
    observation_period_start_date date NOT NULL,
    observation_period_end_date date NOT NULL,
    period_type_concept_id integer NOT NULL
);


ALTER TABLE public.observation_period OWNER TO postgres;

--
-- Name: payer_plan_period; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.payer_plan_period (
    payer_plan_period_id integer NOT NULL,
    person_id integer NOT NULL,
    payer_plan_period_start_date date NOT NULL,
    payer_plan_period_end_date date NOT NULL,
    payer_concept_id integer,
    payer_source_value character varying(50),
    payer_source_concept_id integer,
    plan_concept_id integer,
    plan_source_value character varying(50),
    plan_source_concept_id integer,
    sponsor_concept_id integer,
    sponsor_source_value character varying(50),
    sponsor_source_concept_id integer,
    family_source_value character varying(50),
    stop_reason_concept_id integer,
    stop_reason_source_value character varying(50),
    stop_reason_source_concept_id integer
);


ALTER TABLE public.payer_plan_period OWNER TO postgres;

--
-- Name: person; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.person (
    person_id integer NOT NULL,
    gender_concept_id integer NOT NULL,
    year_of_birth integer NOT NULL,
    month_of_birth integer,
    day_of_birth integer,
    birth_datetime timestamp without time zone,
    race_concept_id integer NOT NULL,
    ethnicity_concept_id integer NOT NULL,
    location_id integer,
    provider_id integer,
    care_site_id integer,
    person_source_value character varying(50),
    gender_source_value character varying(50),
    gender_source_concept_id integer,
    race_source_value character varying(50),
    race_source_concept_id integer,
    ethnicity_source_value character varying(50),
    ethnicity_source_concept_id integer
);


ALTER TABLE public.person OWNER TO postgres;

--
-- Name: procedure_occurrence; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.procedure_occurrence (
    procedure_occurrence_id integer NOT NULL,
    person_id integer NOT NULL,
    procedure_concept_id integer NOT NULL,
    procedure_date date NOT NULL,
    procedure_datetime timestamp without time zone,
    procedure_end_date date,
    procedure_end_datetime timestamp without time zone,
    procedure_type_concept_id integer NOT NULL,
    modifier_concept_id integer,
    quantity integer,
    provider_id integer,
    visit_occurrence_id integer,
    visit_detail_id integer,
    procedure_source_value character varying(50),
    procedure_source_concept_id integer,
    modifier_source_value character varying(50)
);


ALTER TABLE public.procedure_occurrence OWNER TO postgres;

--
-- Name: provider; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.provider (
    provider_id integer NOT NULL,
    provider_name character varying(255),
    npi character varying(20),
    dea character varying(20),
    specialty_concept_id integer,
    care_site_id integer,
    year_of_birth integer,
    gender_concept_id integer,
    provider_source_value character varying(50),
    specialty_source_value character varying(50),
    specialty_source_concept_id integer,
    gender_source_value character varying(50),
    gender_source_concept_id integer
);


ALTER TABLE public.provider OWNER TO postgres;

--
-- Name: relationship; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.relationship (
    relationship_id character varying(20) NOT NULL,
    relationship_name character varying(255) NOT NULL,
    is_hierarchical character varying(1) NOT NULL,
    defines_ancestry character varying(1) NOT NULL,
    reverse_relationship_id character varying(20) NOT NULL,
    relationship_concept_id integer NOT NULL
);


ALTER TABLE public.relationship OWNER TO postgres;

--
-- Name: source_to_concept_map; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.source_to_concept_map (
    source_code character varying(50) NOT NULL,
    source_concept_id integer NOT NULL,
    source_vocabulary_id character varying(20) NOT NULL,
    source_code_description character varying(255),
    target_concept_id integer NOT NULL,
    target_vocabulary_id character varying(20) NOT NULL,
    valid_start_date date NOT NULL,
    valid_end_date date NOT NULL,
    invalid_reason character varying(1)
);


ALTER TABLE public.source_to_concept_map OWNER TO postgres;

--
-- Name: specimen; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.specimen (
    specimen_id integer NOT NULL,
    person_id integer NOT NULL,
    specimen_concept_id integer NOT NULL,
    specimen_type_concept_id integer NOT NULL,
    specimen_date date NOT NULL,
    specimen_datetime timestamp without time zone,
    quantity numeric,
    unit_concept_id integer,
    anatomic_site_concept_id integer,
    disease_status_concept_id integer,
    specimen_source_id character varying(50),
    specimen_source_value character varying(50),
    unit_source_value character varying(50),
    anatomic_site_source_value character varying(50),
    disease_status_source_value character varying(50)
);


ALTER TABLE public.specimen OWNER TO postgres;

--
-- Name: visit_detail; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.visit_detail (
    visit_detail_id integer NOT NULL,
    person_id integer NOT NULL,
    visit_detail_concept_id integer NOT NULL,
    visit_detail_start_date date NOT NULL,
    visit_detail_start_datetime timestamp without time zone,
    visit_detail_end_date date NOT NULL,
    visit_detail_end_datetime timestamp without time zone,
    visit_detail_type_concept_id integer NOT NULL,
    provider_id integer,
    care_site_id integer,
    visit_detail_source_value character varying(50),
    visit_detail_source_concept_id integer,
    admitted_from_concept_id integer,
    admitted_from_source_value character varying(50),
    discharged_to_source_value character varying(50),
    discharged_to_concept_id integer,
    preceding_visit_detail_id integer,
    parent_visit_detail_id integer,
    visit_occurrence_id integer NOT NULL
);


ALTER TABLE public.visit_detail OWNER TO postgres;

--
-- Name: visit_occurrence; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.visit_occurrence (
    visit_occurrence_id integer NOT NULL,
    person_id integer NOT NULL,
    visit_concept_id integer NOT NULL,
    visit_start_date date NOT NULL,
    visit_start_datetime timestamp without time zone,
    visit_end_date date NOT NULL,
    visit_end_datetime timestamp without time zone,
    visit_type_concept_id integer NOT NULL,
    provider_id integer,
    care_site_id integer,
    visit_source_value character varying(50),
    visit_source_concept_id integer,
    admitted_from_concept_id integer,
    admitted_from_source_value character varying(50),
    discharged_to_concept_id integer,
    discharged_to_source_value character varying(50),
    preceding_visit_occurrence_id integer
);


ALTER TABLE public.visit_occurrence OWNER TO postgres;

--
-- Name: vocabulary; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.vocabulary (
    vocabulary_id character varying(20) NOT NULL,
    vocabulary_name character varying(255) NOT NULL,
    vocabulary_reference character varying(255),
    vocabulary_version character varying(255),
    vocabulary_concept_id integer NOT NULL
);


ALTER TABLE public.vocabulary OWNER TO postgres;

--
-- Name: care_site xpk_care_site; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.care_site
    ADD CONSTRAINT xpk_care_site PRIMARY KEY (care_site_id);


--
-- Name: concept xpk_concept; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept
    ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);


--
-- Name: concept_class xpk_concept_class; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_class
    ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);


--
-- Name: condition_era xpk_condition_era; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_era
    ADD CONSTRAINT xpk_condition_era PRIMARY KEY (condition_era_id);


--
-- Name: condition_occurrence xpk_condition_occurrence; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT xpk_condition_occurrence PRIMARY KEY (condition_occurrence_id);


--
-- Name: cost xpk_cost; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost
    ADD CONSTRAINT xpk_cost PRIMARY KEY (cost_id);


--
-- Name: device_exposure xpk_device_exposure; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT xpk_device_exposure PRIMARY KEY (device_exposure_id);


--
-- Name: domain xpk_domain; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domain
    ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);


--
-- Name: dose_era xpk_dose_era; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dose_era
    ADD CONSTRAINT xpk_dose_era PRIMARY KEY (dose_era_id);


--
-- Name: drug_era xpk_drug_era; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_era
    ADD CONSTRAINT xpk_drug_era PRIMARY KEY (drug_era_id);


--
-- Name: drug_exposure xpk_drug_exposure; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT xpk_drug_exposure PRIMARY KEY (drug_exposure_id);


--
-- Name: episode xpk_episode; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode
    ADD CONSTRAINT xpk_episode PRIMARY KEY (episode_id);


--
-- Name: location xpk_location; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.location
    ADD CONSTRAINT xpk_location PRIMARY KEY (location_id);


--
-- Name: measurement xpk_measurement; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT xpk_measurement PRIMARY KEY (measurement_id);


--
-- Name: metadata xpk_metadata; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT xpk_metadata PRIMARY KEY (metadata_id);


--
-- Name: note xpk_note; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT xpk_note PRIMARY KEY (note_id);


--
-- Name: note_nlp xpk_note_nlp; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note_nlp
    ADD CONSTRAINT xpk_note_nlp PRIMARY KEY (note_nlp_id);


--
-- Name: observation xpk_observation; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT xpk_observation PRIMARY KEY (observation_id);


--
-- Name: observation_period xpk_observation_period; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation_period
    ADD CONSTRAINT xpk_observation_period PRIMARY KEY (observation_period_id);


--
-- Name: payer_plan_period xpk_payer_plan_period; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT xpk_payer_plan_period PRIMARY KEY (payer_plan_period_id);


--
-- Name: person xpk_person; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT xpk_person PRIMARY KEY (person_id);


--
-- Name: procedure_occurrence xpk_procedure_occurrence; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT xpk_procedure_occurrence PRIMARY KEY (procedure_occurrence_id);


--
-- Name: provider xpk_provider; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT xpk_provider PRIMARY KEY (provider_id);


--
-- Name: relationship xpk_relationship; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.relationship
    ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);


--
-- Name: specimen xpk_specimen; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT xpk_specimen PRIMARY KEY (specimen_id);


--
-- Name: visit_detail xpk_visit_detail; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT xpk_visit_detail PRIMARY KEY (visit_detail_id);


--
-- Name: visit_occurrence xpk_visit_occurrence; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT xpk_visit_occurrence PRIMARY KEY (visit_occurrence_id);


--
-- Name: vocabulary xpk_vocabulary; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vocabulary
    ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);


--
-- Name: idx_care_site_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_care_site_id_1 ON public.care_site USING btree (care_site_id);

ALTER TABLE public.care_site CLUSTER ON idx_care_site_id_1;


--
-- Name: idx_concept_ancestor_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_ancestor_id_1 ON public.concept_ancestor USING btree (ancestor_concept_id);

ALTER TABLE public.concept_ancestor CLUSTER ON idx_concept_ancestor_id_1;


--
-- Name: idx_concept_ancestor_id_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_ancestor_id_2 ON public.concept_ancestor USING btree (descendant_concept_id);


--
-- Name: idx_concept_class_class_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_class_class_id ON public.concept_class USING btree (concept_class_id);

ALTER TABLE public.concept_class CLUSTER ON idx_concept_class_class_id;


--
-- Name: idx_concept_class_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_class_id ON public.concept USING btree (concept_class_id);


--
-- Name: idx_concept_code; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_code ON public.concept USING btree (concept_code);


--
-- Name: idx_concept_concept_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_concept_id ON public.concept USING btree (concept_id);

ALTER TABLE public.concept CLUSTER ON idx_concept_concept_id;


--
-- Name: idx_concept_domain_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_domain_id ON public.concept USING btree (domain_id);


--
-- Name: idx_concept_relationship_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_relationship_id_1 ON public.concept_relationship USING btree (concept_id_1);

ALTER TABLE public.concept_relationship CLUSTER ON idx_concept_relationship_id_1;


--
-- Name: idx_concept_relationship_id_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_relationship_id_2 ON public.concept_relationship USING btree (concept_id_2);


--
-- Name: idx_concept_relationship_id_3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_relationship_id_3 ON public.concept_relationship USING btree (relationship_id);


--
-- Name: idx_concept_synonym_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_synonym_id ON public.concept_synonym USING btree (concept_id);

ALTER TABLE public.concept_synonym CLUSTER ON idx_concept_synonym_id;


--
-- Name: idx_concept_vocabluary_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_concept_vocabluary_id ON public.concept USING btree (vocabulary_id);


--
-- Name: idx_condition_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_condition_concept_id_1 ON public.condition_occurrence USING btree (condition_concept_id);


--
-- Name: idx_condition_era_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_condition_era_concept_id_1 ON public.condition_era USING btree (condition_concept_id);


--
-- Name: idx_condition_era_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_condition_era_person_id_1 ON public.condition_era USING btree (person_id);

ALTER TABLE public.condition_era CLUSTER ON idx_condition_era_person_id_1;


--
-- Name: idx_condition_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_condition_person_id_1 ON public.condition_occurrence USING btree (person_id);

ALTER TABLE public.condition_occurrence CLUSTER ON idx_condition_person_id_1;


--
-- Name: idx_condition_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_condition_visit_id_1 ON public.condition_occurrence USING btree (visit_occurrence_id);


--
-- Name: idx_cost_event_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_cost_event_id ON public.cost USING btree (cost_event_id);


--
-- Name: idx_death_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_death_person_id_1 ON public.death USING btree (person_id);

ALTER TABLE public.death CLUSTER ON idx_death_person_id_1;


--
-- Name: idx_device_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_device_concept_id_1 ON public.device_exposure USING btree (device_concept_id);


--
-- Name: idx_device_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_device_person_id_1 ON public.device_exposure USING btree (person_id);

ALTER TABLE public.device_exposure CLUSTER ON idx_device_person_id_1;


--
-- Name: idx_device_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_device_visit_id_1 ON public.device_exposure USING btree (visit_occurrence_id);


--
-- Name: idx_domain_domain_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_domain_domain_id ON public.domain USING btree (domain_id);

ALTER TABLE public.domain CLUSTER ON idx_domain_domain_id;


--
-- Name: idx_dose_era_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dose_era_concept_id_1 ON public.dose_era USING btree (drug_concept_id);


--
-- Name: idx_dose_era_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_dose_era_person_id_1 ON public.dose_era USING btree (person_id);

ALTER TABLE public.dose_era CLUSTER ON idx_dose_era_person_id_1;


--
-- Name: idx_drug_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_concept_id_1 ON public.drug_exposure USING btree (drug_concept_id);


--
-- Name: idx_drug_era_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_era_concept_id_1 ON public.drug_era USING btree (drug_concept_id);


--
-- Name: idx_drug_era_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_era_person_id_1 ON public.drug_era USING btree (person_id);

ALTER TABLE public.drug_era CLUSTER ON idx_drug_era_person_id_1;


--
-- Name: idx_drug_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_person_id_1 ON public.drug_exposure USING btree (person_id);

ALTER TABLE public.drug_exposure CLUSTER ON idx_drug_person_id_1;


--
-- Name: idx_drug_strength_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_strength_id_1 ON public.drug_strength USING btree (drug_concept_id);

ALTER TABLE public.drug_strength CLUSTER ON idx_drug_strength_id_1;


--
-- Name: idx_drug_strength_id_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_strength_id_2 ON public.drug_strength USING btree (ingredient_concept_id);


--
-- Name: idx_drug_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_drug_visit_id_1 ON public.drug_exposure USING btree (visit_occurrence_id);


--
-- Name: idx_fact_relationship_id1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_relationship_id1 ON public.fact_relationship USING btree (domain_concept_id_1);


--
-- Name: idx_fact_relationship_id2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_relationship_id2 ON public.fact_relationship USING btree (domain_concept_id_2);


--
-- Name: idx_fact_relationship_id3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fact_relationship_id3 ON public.fact_relationship USING btree (relationship_concept_id);


--
-- Name: idx_gender; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_gender ON public.person USING btree (gender_concept_id);


--
-- Name: idx_location_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_location_id_1 ON public.location USING btree (location_id);

ALTER TABLE public.location CLUSTER ON idx_location_id_1;


--
-- Name: idx_measurement_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_measurement_concept_id_1 ON public.measurement USING btree (measurement_concept_id);


--
-- Name: idx_measurement_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_measurement_person_id_1 ON public.measurement USING btree (person_id);

ALTER TABLE public.measurement CLUSTER ON idx_measurement_person_id_1;


--
-- Name: idx_measurement_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_measurement_visit_id_1 ON public.measurement USING btree (visit_occurrence_id);


--
-- Name: idx_metadata_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_metadata_concept_id_1 ON public.metadata USING btree (metadata_concept_id);

ALTER TABLE public.metadata CLUSTER ON idx_metadata_concept_id_1;


--
-- Name: idx_note_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_note_concept_id_1 ON public.note USING btree (note_type_concept_id);


--
-- Name: idx_note_nlp_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_note_nlp_concept_id_1 ON public.note_nlp USING btree (note_nlp_concept_id);


--
-- Name: idx_note_nlp_note_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_note_nlp_note_id_1 ON public.note_nlp USING btree (note_id);

ALTER TABLE public.note_nlp CLUSTER ON idx_note_nlp_note_id_1;


--
-- Name: idx_note_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_note_person_id_1 ON public.note USING btree (person_id);

ALTER TABLE public.note CLUSTER ON idx_note_person_id_1;


--
-- Name: idx_note_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_note_visit_id_1 ON public.note USING btree (visit_occurrence_id);


--
-- Name: idx_observation_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_observation_concept_id_1 ON public.observation USING btree (observation_concept_id);


--
-- Name: idx_observation_period_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_observation_period_id_1 ON public.observation_period USING btree (person_id);

ALTER TABLE public.observation_period CLUSTER ON idx_observation_period_id_1;


--
-- Name: idx_observation_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_observation_person_id_1 ON public.observation USING btree (person_id);

ALTER TABLE public.observation CLUSTER ON idx_observation_person_id_1;


--
-- Name: idx_observation_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_observation_visit_id_1 ON public.observation USING btree (visit_occurrence_id);


--
-- Name: idx_period_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_period_person_id_1 ON public.payer_plan_period USING btree (person_id);

ALTER TABLE public.payer_plan_period CLUSTER ON idx_period_person_id_1;


--
-- Name: idx_person_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_person_id ON public.person USING btree (person_id);

ALTER TABLE public.person CLUSTER ON idx_person_id;


--
-- Name: idx_procedure_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_procedure_concept_id_1 ON public.procedure_occurrence USING btree (procedure_concept_id);


--
-- Name: idx_procedure_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_procedure_person_id_1 ON public.procedure_occurrence USING btree (person_id);

ALTER TABLE public.procedure_occurrence CLUSTER ON idx_procedure_person_id_1;


--
-- Name: idx_procedure_visit_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_procedure_visit_id_1 ON public.procedure_occurrence USING btree (visit_occurrence_id);


--
-- Name: idx_provider_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_provider_id_1 ON public.provider USING btree (provider_id);

ALTER TABLE public.provider CLUSTER ON idx_provider_id_1;


--
-- Name: idx_relationship_rel_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_relationship_rel_id ON public.relationship USING btree (relationship_id);

ALTER TABLE public.relationship CLUSTER ON idx_relationship_rel_id;


--
-- Name: idx_source_to_concept_map_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_source_to_concept_map_1 ON public.source_to_concept_map USING btree (source_vocabulary_id);


--
-- Name: idx_source_to_concept_map_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_source_to_concept_map_2 ON public.source_to_concept_map USING btree (target_vocabulary_id);


--
-- Name: idx_source_to_concept_map_3; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_source_to_concept_map_3 ON public.source_to_concept_map USING btree (target_concept_id);

ALTER TABLE public.source_to_concept_map CLUSTER ON idx_source_to_concept_map_3;


--
-- Name: idx_source_to_concept_map_c; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_source_to_concept_map_c ON public.source_to_concept_map USING btree (source_code);


--
-- Name: idx_specimen_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_specimen_concept_id_1 ON public.specimen USING btree (specimen_concept_id);


--
-- Name: idx_specimen_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_specimen_person_id_1 ON public.specimen USING btree (person_id);

ALTER TABLE public.specimen CLUSTER ON idx_specimen_person_id_1;


--
-- Name: idx_visit_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_visit_concept_id_1 ON public.visit_occurrence USING btree (visit_concept_id);


--
-- Name: idx_visit_det_concept_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_visit_det_concept_id_1 ON public.visit_detail USING btree (visit_detail_concept_id);


--
-- Name: idx_visit_det_occ_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_visit_det_occ_id ON public.visit_detail USING btree (visit_occurrence_id);


--
-- Name: idx_visit_det_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_visit_det_person_id_1 ON public.visit_detail USING btree (person_id);

ALTER TABLE public.visit_detail CLUSTER ON idx_visit_det_person_id_1;


--
-- Name: idx_visit_person_id_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_visit_person_id_1 ON public.visit_occurrence USING btree (person_id);

ALTER TABLE public.visit_occurrence CLUSTER ON idx_visit_person_id_1;


--
-- Name: idx_vocabulary_vocabulary_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_vocabulary_vocabulary_id ON public.vocabulary USING btree (vocabulary_id);

ALTER TABLE public.vocabulary CLUSTER ON idx_vocabulary_vocabulary_id;


--
-- Name: care_site fpk_care_site_location_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.care_site
    ADD CONSTRAINT fpk_care_site_location_id FOREIGN KEY (location_id) REFERENCES public.location(location_id);


--
-- Name: care_site fpk_care_site_place_of_service_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.care_site
    ADD CONSTRAINT fpk_care_site_place_of_service_concept_id FOREIGN KEY (place_of_service_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: cdm_source fpk_cdm_source_cdm_version_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cdm_source
    ADD CONSTRAINT fpk_cdm_source_cdm_version_concept_id FOREIGN KEY (cdm_version_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: cohort_definition fpk_cohort_definition_definition_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cohort_definition
    ADD CONSTRAINT fpk_cohort_definition_definition_type_concept_id FOREIGN KEY (definition_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: cohort_definition fpk_cohort_definition_subject_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cohort_definition
    ADD CONSTRAINT fpk_cohort_definition_subject_concept_id FOREIGN KEY (subject_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: concept_ancestor fpk_concept_ancestor_ancestor_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_ancestor
    ADD CONSTRAINT fpk_concept_ancestor_ancestor_concept_id FOREIGN KEY (ancestor_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: concept_ancestor fpk_concept_ancestor_descendant_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_ancestor
    ADD CONSTRAINT fpk_concept_ancestor_descendant_concept_id FOREIGN KEY (descendant_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: concept_class fpk_concept_class_concept_class_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_class
    ADD CONSTRAINT fpk_concept_class_concept_class_concept_id FOREIGN KEY (concept_class_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: concept fpk_concept_concept_class_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept
    ADD CONSTRAINT fpk_concept_concept_class_id FOREIGN KEY (concept_class_id) REFERENCES public.concept_class(concept_class_id);


--
-- Name: concept fpk_concept_domain_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept
    ADD CONSTRAINT fpk_concept_domain_id FOREIGN KEY (domain_id) REFERENCES public.domain(domain_id);


--
-- Name: concept_relationship fpk_concept_relationship_concept_id_1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_relationship
    ADD CONSTRAINT fpk_concept_relationship_concept_id_1 FOREIGN KEY (concept_id_1) REFERENCES public.concept(concept_id);


--
-- Name: concept_relationship fpk_concept_relationship_concept_id_2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_relationship
    ADD CONSTRAINT fpk_concept_relationship_concept_id_2 FOREIGN KEY (concept_id_2) REFERENCES public.concept(concept_id);


--
-- Name: concept_relationship fpk_concept_relationship_relationship_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_relationship
    ADD CONSTRAINT fpk_concept_relationship_relationship_id FOREIGN KEY (relationship_id) REFERENCES public.relationship(relationship_id);


--
-- Name: concept_synonym fpk_concept_synonym_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_synonym
    ADD CONSTRAINT fpk_concept_synonym_concept_id FOREIGN KEY (concept_id) REFERENCES public.concept(concept_id);


--
-- Name: concept_synonym fpk_concept_synonym_language_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept_synonym
    ADD CONSTRAINT fpk_concept_synonym_language_concept_id FOREIGN KEY (language_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: concept fpk_concept_vocabulary_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.concept
    ADD CONSTRAINT fpk_concept_vocabulary_id FOREIGN KEY (vocabulary_id) REFERENCES public.vocabulary(vocabulary_id);


--
-- Name: condition_era fpk_condition_era_condition_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_era
    ADD CONSTRAINT fpk_condition_era_condition_concept_id FOREIGN KEY (condition_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: condition_era fpk_condition_era_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_era
    ADD CONSTRAINT fpk_condition_era_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_condition_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_condition_concept_id FOREIGN KEY (condition_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_condition_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_condition_source_concept_id FOREIGN KEY (condition_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_condition_status_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_condition_status_concept_id FOREIGN KEY (condition_status_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_condition_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_condition_type_concept_id FOREIGN KEY (condition_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: condition_occurrence fpk_condition_occurrence_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.condition_occurrence
    ADD CONSTRAINT fpk_condition_occurrence_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: cost fpk_cost_cost_domain_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost
    ADD CONSTRAINT fpk_cost_cost_domain_id FOREIGN KEY (cost_domain_id) REFERENCES public.domain(domain_id);


--
-- Name: cost fpk_cost_cost_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost
    ADD CONSTRAINT fpk_cost_cost_type_concept_id FOREIGN KEY (cost_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: cost fpk_cost_currency_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost
    ADD CONSTRAINT fpk_cost_currency_concept_id FOREIGN KEY (currency_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: cost fpk_cost_drg_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost
    ADD CONSTRAINT fpk_cost_drg_concept_id FOREIGN KEY (drg_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: cost fpk_cost_revenue_code_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost
    ADD CONSTRAINT fpk_cost_revenue_code_concept_id FOREIGN KEY (revenue_code_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: death fpk_death_cause_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.death
    ADD CONSTRAINT fpk_death_cause_concept_id FOREIGN KEY (cause_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: death fpk_death_cause_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.death
    ADD CONSTRAINT fpk_death_cause_source_concept_id FOREIGN KEY (cause_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: death fpk_death_death_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.death
    ADD CONSTRAINT fpk_death_death_type_concept_id FOREIGN KEY (death_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: death fpk_death_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.death
    ADD CONSTRAINT fpk_death_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: device_exposure fpk_device_exposure_device_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_device_concept_id FOREIGN KEY (device_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: device_exposure fpk_device_exposure_device_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_device_source_concept_id FOREIGN KEY (device_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: device_exposure fpk_device_exposure_device_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_device_type_concept_id FOREIGN KEY (device_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: device_exposure fpk_device_exposure_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: device_exposure fpk_device_exposure_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: device_exposure fpk_device_exposure_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_unit_concept_id FOREIGN KEY (unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: device_exposure fpk_device_exposure_unit_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_unit_source_concept_id FOREIGN KEY (unit_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: device_exposure fpk_device_exposure_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: device_exposure fpk_device_exposure_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.device_exposure
    ADD CONSTRAINT fpk_device_exposure_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: domain fpk_domain_domain_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domain
    ADD CONSTRAINT fpk_domain_domain_concept_id FOREIGN KEY (domain_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: dose_era fpk_dose_era_drug_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dose_era
    ADD CONSTRAINT fpk_dose_era_drug_concept_id FOREIGN KEY (drug_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: dose_era fpk_dose_era_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dose_era
    ADD CONSTRAINT fpk_dose_era_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: dose_era fpk_dose_era_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dose_era
    ADD CONSTRAINT fpk_dose_era_unit_concept_id FOREIGN KEY (unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_era fpk_drug_era_drug_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_era
    ADD CONSTRAINT fpk_drug_era_drug_concept_id FOREIGN KEY (drug_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_era fpk_drug_era_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_era
    ADD CONSTRAINT fpk_drug_era_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: drug_exposure fpk_drug_exposure_drug_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_drug_concept_id FOREIGN KEY (drug_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_exposure fpk_drug_exposure_drug_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_drug_source_concept_id FOREIGN KEY (drug_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_exposure fpk_drug_exposure_drug_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_drug_type_concept_id FOREIGN KEY (drug_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_exposure fpk_drug_exposure_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: drug_exposure fpk_drug_exposure_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: drug_exposure fpk_drug_exposure_route_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_route_concept_id FOREIGN KEY (route_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_exposure fpk_drug_exposure_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: drug_exposure fpk_drug_exposure_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_exposure
    ADD CONSTRAINT fpk_drug_exposure_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: drug_strength fpk_drug_strength_amount_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_strength
    ADD CONSTRAINT fpk_drug_strength_amount_unit_concept_id FOREIGN KEY (amount_unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_strength fpk_drug_strength_denominator_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_strength
    ADD CONSTRAINT fpk_drug_strength_denominator_unit_concept_id FOREIGN KEY (denominator_unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_strength fpk_drug_strength_drug_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_strength
    ADD CONSTRAINT fpk_drug_strength_drug_concept_id FOREIGN KEY (drug_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_strength fpk_drug_strength_ingredient_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_strength
    ADD CONSTRAINT fpk_drug_strength_ingredient_concept_id FOREIGN KEY (ingredient_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: drug_strength fpk_drug_strength_numerator_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.drug_strength
    ADD CONSTRAINT fpk_drug_strength_numerator_unit_concept_id FOREIGN KEY (numerator_unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: episode fpk_episode_episode_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode
    ADD CONSTRAINT fpk_episode_episode_concept_id FOREIGN KEY (episode_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: episode fpk_episode_episode_object_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode
    ADD CONSTRAINT fpk_episode_episode_object_concept_id FOREIGN KEY (episode_object_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: episode fpk_episode_episode_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode
    ADD CONSTRAINT fpk_episode_episode_source_concept_id FOREIGN KEY (episode_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: episode fpk_episode_episode_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode
    ADD CONSTRAINT fpk_episode_episode_type_concept_id FOREIGN KEY (episode_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: episode_event fpk_episode_event_episode_event_field_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode_event
    ADD CONSTRAINT fpk_episode_event_episode_event_field_concept_id FOREIGN KEY (episode_event_field_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: episode_event fpk_episode_event_episode_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode_event
    ADD CONSTRAINT fpk_episode_event_episode_id FOREIGN KEY (episode_id) REFERENCES public.episode(episode_id);


--
-- Name: episode fpk_episode_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.episode
    ADD CONSTRAINT fpk_episode_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: fact_relationship fpk_fact_relationship_domain_concept_id_1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_relationship
    ADD CONSTRAINT fpk_fact_relationship_domain_concept_id_1 FOREIGN KEY (domain_concept_id_1) REFERENCES public.concept(concept_id);


--
-- Name: fact_relationship fpk_fact_relationship_domain_concept_id_2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_relationship
    ADD CONSTRAINT fpk_fact_relationship_domain_concept_id_2 FOREIGN KEY (domain_concept_id_2) REFERENCES public.concept(concept_id);


--
-- Name: fact_relationship fpk_fact_relationship_relationship_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_relationship
    ADD CONSTRAINT fpk_fact_relationship_relationship_concept_id FOREIGN KEY (relationship_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: location fpk_location_country_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.location
    ADD CONSTRAINT fpk_location_country_concept_id FOREIGN KEY (country_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_meas_event_field_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_meas_event_field_concept_id FOREIGN KEY (meas_event_field_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_measurement_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_measurement_concept_id FOREIGN KEY (measurement_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_measurement_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_measurement_source_concept_id FOREIGN KEY (measurement_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_measurement_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_measurement_type_concept_id FOREIGN KEY (measurement_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_operator_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_operator_concept_id FOREIGN KEY (operator_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: measurement fpk_measurement_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: measurement fpk_measurement_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_unit_concept_id FOREIGN KEY (unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_unit_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_unit_source_concept_id FOREIGN KEY (unit_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_value_as_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_value_as_concept_id FOREIGN KEY (value_as_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: measurement fpk_measurement_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: measurement fpk_measurement_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fpk_measurement_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: metadata fpk_metadata_metadata_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT fpk_metadata_metadata_concept_id FOREIGN KEY (metadata_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: metadata fpk_metadata_metadata_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT fpk_metadata_metadata_type_concept_id FOREIGN KEY (metadata_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: metadata fpk_metadata_value_as_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT fpk_metadata_value_as_concept_id FOREIGN KEY (value_as_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note fpk_note_encoding_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_encoding_concept_id FOREIGN KEY (encoding_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note fpk_note_language_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_language_concept_id FOREIGN KEY (language_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note_nlp fpk_note_nlp_note_nlp_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note_nlp
    ADD CONSTRAINT fpk_note_nlp_note_nlp_concept_id FOREIGN KEY (note_nlp_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note_nlp fpk_note_nlp_note_nlp_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note_nlp
    ADD CONSTRAINT fpk_note_nlp_note_nlp_source_concept_id FOREIGN KEY (note_nlp_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note_nlp fpk_note_nlp_section_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note_nlp
    ADD CONSTRAINT fpk_note_nlp_section_concept_id FOREIGN KEY (section_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note fpk_note_note_class_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_note_class_concept_id FOREIGN KEY (note_class_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note fpk_note_note_event_field_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_note_event_field_concept_id FOREIGN KEY (note_event_field_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note fpk_note_note_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_note_type_concept_id FOREIGN KEY (note_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: note fpk_note_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: note fpk_note_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: note fpk_note_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: note fpk_note_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.note
    ADD CONSTRAINT fpk_note_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: observation fpk_observation_obs_event_field_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_obs_event_field_concept_id FOREIGN KEY (obs_event_field_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation fpk_observation_observation_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_observation_concept_id FOREIGN KEY (observation_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation fpk_observation_observation_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_observation_source_concept_id FOREIGN KEY (observation_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation fpk_observation_observation_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_observation_type_concept_id FOREIGN KEY (observation_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation_period fpk_observation_period_period_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation_period
    ADD CONSTRAINT fpk_observation_period_period_type_concept_id FOREIGN KEY (period_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation_period fpk_observation_period_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation_period
    ADD CONSTRAINT fpk_observation_period_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: observation fpk_observation_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: observation fpk_observation_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: observation fpk_observation_qualifier_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_qualifier_concept_id FOREIGN KEY (qualifier_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation fpk_observation_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_unit_concept_id FOREIGN KEY (unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation fpk_observation_value_as_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_value_as_concept_id FOREIGN KEY (value_as_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: observation fpk_observation_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: observation fpk_observation_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.observation
    ADD CONSTRAINT fpk_observation_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_payer_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_payer_concept_id FOREIGN KEY (payer_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_payer_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_payer_source_concept_id FOREIGN KEY (payer_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_plan_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_plan_concept_id FOREIGN KEY (plan_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_plan_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_plan_source_concept_id FOREIGN KEY (plan_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_sponsor_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_sponsor_concept_id FOREIGN KEY (sponsor_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_sponsor_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_sponsor_source_concept_id FOREIGN KEY (sponsor_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_stop_reason_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_stop_reason_concept_id FOREIGN KEY (stop_reason_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: payer_plan_period fpk_payer_plan_period_stop_reason_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.payer_plan_period
    ADD CONSTRAINT fpk_payer_plan_period_stop_reason_source_concept_id FOREIGN KEY (stop_reason_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: person fpk_person_care_site_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_care_site_id FOREIGN KEY (care_site_id) REFERENCES public.care_site(care_site_id);


--
-- Name: person fpk_person_ethnicity_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_ethnicity_concept_id FOREIGN KEY (ethnicity_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: person fpk_person_ethnicity_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_ethnicity_source_concept_id FOREIGN KEY (ethnicity_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: person fpk_person_gender_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_gender_concept_id FOREIGN KEY (gender_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: person fpk_person_gender_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_gender_source_concept_id FOREIGN KEY (gender_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: person fpk_person_location_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_location_id FOREIGN KEY (location_id) REFERENCES public.location(location_id);


--
-- Name: person fpk_person_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: person fpk_person_race_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_race_concept_id FOREIGN KEY (race_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: person fpk_person_race_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT fpk_person_race_source_concept_id FOREIGN KEY (race_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_modifier_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_modifier_concept_id FOREIGN KEY (modifier_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_procedure_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_procedure_concept_id FOREIGN KEY (procedure_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_procedure_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_procedure_source_concept_id FOREIGN KEY (procedure_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_procedure_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_procedure_type_concept_id FOREIGN KEY (procedure_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_visit_detail_id FOREIGN KEY (visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: procedure_occurrence fpk_procedure_occurrence_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.procedure_occurrence
    ADD CONSTRAINT fpk_procedure_occurrence_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: provider fpk_provider_care_site_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT fpk_provider_care_site_id FOREIGN KEY (care_site_id) REFERENCES public.care_site(care_site_id);


--
-- Name: provider fpk_provider_gender_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT fpk_provider_gender_concept_id FOREIGN KEY (gender_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: provider fpk_provider_gender_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT fpk_provider_gender_source_concept_id FOREIGN KEY (gender_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: provider fpk_provider_specialty_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT fpk_provider_specialty_concept_id FOREIGN KEY (specialty_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: provider fpk_provider_specialty_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.provider
    ADD CONSTRAINT fpk_provider_specialty_source_concept_id FOREIGN KEY (specialty_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: relationship fpk_relationship_relationship_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.relationship
    ADD CONSTRAINT fpk_relationship_relationship_concept_id FOREIGN KEY (relationship_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: source_to_concept_map fpk_source_to_concept_map_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.source_to_concept_map
    ADD CONSTRAINT fpk_source_to_concept_map_source_concept_id FOREIGN KEY (source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: source_to_concept_map fpk_source_to_concept_map_target_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.source_to_concept_map
    ADD CONSTRAINT fpk_source_to_concept_map_target_concept_id FOREIGN KEY (target_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: source_to_concept_map fpk_source_to_concept_map_target_vocabulary_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.source_to_concept_map
    ADD CONSTRAINT fpk_source_to_concept_map_target_vocabulary_id FOREIGN KEY (target_vocabulary_id) REFERENCES public.vocabulary(vocabulary_id);


--
-- Name: specimen fpk_specimen_anatomic_site_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT fpk_specimen_anatomic_site_concept_id FOREIGN KEY (anatomic_site_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: specimen fpk_specimen_disease_status_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT fpk_specimen_disease_status_concept_id FOREIGN KEY (disease_status_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: specimen fpk_specimen_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT fpk_specimen_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: specimen fpk_specimen_specimen_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT fpk_specimen_specimen_concept_id FOREIGN KEY (specimen_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: specimen fpk_specimen_specimen_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT fpk_specimen_specimen_type_concept_id FOREIGN KEY (specimen_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: specimen fpk_specimen_unit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.specimen
    ADD CONSTRAINT fpk_specimen_unit_concept_id FOREIGN KEY (unit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_detail fpk_visit_detail_admitted_from_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_admitted_from_concept_id FOREIGN KEY (admitted_from_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_detail fpk_visit_detail_care_site_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_care_site_id FOREIGN KEY (care_site_id) REFERENCES public.care_site(care_site_id);


--
-- Name: visit_detail fpk_visit_detail_discharged_to_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_discharged_to_concept_id FOREIGN KEY (discharged_to_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_detail fpk_visit_detail_parent_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_parent_visit_detail_id FOREIGN KEY (parent_visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: visit_detail fpk_visit_detail_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: visit_detail fpk_visit_detail_preceding_visit_detail_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_preceding_visit_detail_id FOREIGN KEY (preceding_visit_detail_id) REFERENCES public.visit_detail(visit_detail_id);


--
-- Name: visit_detail fpk_visit_detail_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: visit_detail fpk_visit_detail_visit_detail_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_visit_detail_concept_id FOREIGN KEY (visit_detail_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_detail fpk_visit_detail_visit_detail_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_visit_detail_source_concept_id FOREIGN KEY (visit_detail_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_detail fpk_visit_detail_visit_detail_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_visit_detail_type_concept_id FOREIGN KEY (visit_detail_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_detail fpk_visit_detail_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_detail
    ADD CONSTRAINT fpk_visit_detail_visit_occurrence_id FOREIGN KEY (visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_admitted_from_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_admitted_from_concept_id FOREIGN KEY (admitted_from_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_care_site_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_care_site_id FOREIGN KEY (care_site_id) REFERENCES public.care_site(care_site_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_discharged_to_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_discharged_to_concept_id FOREIGN KEY (discharged_to_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_person_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_person_id FOREIGN KEY (person_id) REFERENCES public.person(person_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_preceding_visit_occurrence_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_preceding_visit_occurrence_id FOREIGN KEY (preceding_visit_occurrence_id) REFERENCES public.visit_occurrence(visit_occurrence_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_provider_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_provider_id FOREIGN KEY (provider_id) REFERENCES public.provider(provider_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_visit_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_visit_concept_id FOREIGN KEY (visit_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_visit_source_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_visit_source_concept_id FOREIGN KEY (visit_source_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: visit_occurrence fpk_visit_occurrence_visit_type_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.visit_occurrence
    ADD CONSTRAINT fpk_visit_occurrence_visit_type_concept_id FOREIGN KEY (visit_type_concept_id) REFERENCES public.concept(concept_id);


--
-- Name: vocabulary fpk_vocabulary_vocabulary_concept_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vocabulary
    ADD CONSTRAINT fpk_vocabulary_vocabulary_concept_id FOREIGN KEY (vocabulary_concept_id) REFERENCES public.concept(concept_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 9KfiVTbB6sO0pS8kCdE8SivaG1TJpeWniCEpYAKeFGCR9blOGky98HIXeqciz78

