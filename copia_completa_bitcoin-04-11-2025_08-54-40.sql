--
-- PostgreSQL database dump
--

\restrict qLXNffzqtGfuPDSocTkwaaUtEdWgptDGrzsDFT5pXhSGt2OklMmCemHfPsuWTlN

-- Dumped from database version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)

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
-- Name: bitcoin_precios_30; Type: TABLE; Schema: public; Owner: ing_data
--

CREATE TABLE public.bitcoin_precios_30 (
    "Unnamed: 0" bigint,
    fecha text,
    minimo bigint,
    maximo bigint,
    apertura bigint,
    cierre bigint
);


ALTER TABLE public.bitcoin_precios_30 OWNER TO ing_data;

--
-- Data for Name: bitcoin_precios_30; Type: TABLE DATA; Schema: public; Owner: ing_data
--

COPY public.bitcoin_precios_30 ("Unnamed: 0", fecha, minimo, maximo, apertura, cierre) FROM stdin;
1	2025-10-04	122229	122317	122229	122317
2	2025-10-05	122199	125360	122428	123160
3	2025-10-06	123377	126079	123506	125071
4	2025-10-07	120928	125020	124773	121986
5	2025-10-08	121275	123966	121518	123282
6	2025-10-09	120322	123475	123352	121740
7	2025-10-10	113553	121820	121698	113879
8	2025-10-11	110389	113306	113043	111134
9	2025-10-12	109883	115481	110655	115481
10	2025-10-13	114004	115933	115189	115638
11	2025-10-14	110735	115222	115222	113253
12	2025-10-15	110752	113156	113156	110793
13	2025-10-16	107808	111693	110708	107985
14	2025-10-17	104777	109181	108168	107016
15	2025-10-18	106443	107210	106443	107147
16	2025-10-19	106489	109404	107156	108976
17	2025-10-20	107984	111427	108655	110836
18	2025-10-21	107603	113348	110608	109054
19	2025-10-22	107087	108647	108486	107087
20	2025-10-23	107591	111200	107591	109976
21	2025-10-24	109988	111455	110048	110995
22	2025-10-25	110742	111820	110997	111688
23	2025-10-26	111319	114722	111620	114722
24	2025-10-27	114083	115957	114476	114083
25	2025-10-28	112848	115432	114182	113215
26	2025-10-29	110741	113559	112950	111207
27	2025-10-30	106786	111315	110046	107811
28	2025-10-31	108240	110588	108240	109571
29	2025-11-01	109573	110433	109573	110026
30	2025-11-02	109715	111057	110066	109715
31	2025-11-03	105802	110650	110650	106434
\.


--
-- PostgreSQL database dump complete
--

\unrestrict qLXNffzqtGfuPDSocTkwaaUtEdWgptDGrzsDFT5pXhSGt2OklMmCemHfPsuWTlN

