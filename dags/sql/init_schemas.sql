CREATE SCHEMA "source" AUTHORIZATION airflow;
CREATE SCHEMA "stage" AUTHORIZATION airflow;
CREATE SCHEMA "edwh" AUTHORIZATION airflow;

set schema 'source';

CREATE TABLE actor (
    actor_id SERIAL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



CREATE TABLE category (
    category_id SERIAL,
    name character varying(25) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: film; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE film (
    film_id SERIAL,
    title character varying(255) NOT NULL,
    description text,
    release_year integer,
    language_id integer NOT NULL,
    original_language_id integer,
    rental_duration smallint DEFAULT 3 NOT NULL,
    rental_rate numeric(4,2) DEFAULT 4.99 NOT NULL,
    length smallint,
    replacement_cost numeric(5,2) DEFAULT 19.99 NOT NULL,
    rating varchar(12) DEFAULT 'G',
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    special_features text[]
);


-- Name: film_actor; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE film_actor (
    actor_id integer NOT NULL,
    film_id integer NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: film_category; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE film_category (
    film_id integer NOT NULL,
    category_id integer NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: address; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE address (
    address_id SERIAL,
    address character varying(50) NOT NULL,
    address2 character varying(50),
    district character varying(20) NOT NULL,
    city_id integer NOT NULL,
    postal_code character varying(10),
    phone character varying(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

--
-- Name: city_city_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

--
-- Name: city; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE city (
    city_id SERIAL,
    city character varying(50) NOT NULL,
    country_id integer NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: country; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE country (
    country_id SERIAL,
    country character varying(50) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



CREATE TABLE customer (
    customer_id SERIAL,
    store_id integer NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    address_id integer NOT NULL,
    activebool boolean DEFAULT true NOT NULL,
    create_date date DEFAULT ('now'::text)::date NOT NULL,
    last_update timestamp without time zone DEFAULT now(),
    active integer
);


--
-- Name: inventory; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE inventory (
    inventory_id SERIAL,
    film_id integer NOT NULL,
    store_id integer NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: language; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE language (
    language_id SERIAL,
    name character(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: payment; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE payment (
    payment_id SERIAL,
    customer_id integer NOT NULL,
    staff_id integer NOT NULL,
    rental_id integer NOT NULL,
    amount numeric(5,2) NOT NULL,
    payment_date timestamp without time zone NOT NULL
);


--
-- Name: rental; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE rental (
    rental_id SERIAL,
    rental_date timestamp without time zone NOT NULL,
    inventory_id integer NOT NULL,
    customer_id integer NOT NULL,
    return_date timestamp without time zone,
    staff_id integer NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);



--
-- Name: staff; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE staff (
    staff_id SERIAL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    address_id integer NOT NULL,
    email character varying(50),
    store_id integer NOT NULL,
    active boolean DEFAULT true NOT NULL,
    username character varying(16) NOT NULL,
    password character varying(40),
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    picture bytea
);



CREATE TABLE store (
    store_id SERIAL,
    manager_staff_id integer NOT NULL,
    address_id integer NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);


