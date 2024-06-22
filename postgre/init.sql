DROP DATABASE IF EXISTS movies;

CREATE DATABASE movies;

CREATE TABLE IF NOT EXISTS public.movies
(
    id BIGSERIAL PRIMARY KEY,
    original_title text,
    budget BIGINT,
    original_language TEXT,
    revenue FLOAT,
    runtime FLOAT,
    genres jsonb
);

COPY public.movies (id,original_title,budget,original_language,revenue,runtime,genres)
    FROM '..\data-preprocess\data\movies.csv'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE user_movies_temp (
    userId bigint,
    movieId bigint,
    rating float,
    timestamp bigint
);

COPY public.user_movies_temp (userId, movieId, rating, timestamp)
    FROM '..\data-preprocess\data\ratings_small.csv'
    DELIMITER ','
    CSV HEADER;

create table if not exists ratings (
    id bigserial,
    user_id bigint,
    movie_id bigint references public.movies(id),
    rating float
);

insert into ratings(user_id, movie_id, rating)
select userId, movieId, rating from user_movies_temp where exists (select * from public.movies where id = movieId);

drop table user_movies_temp;

create table if not exists actors(
    id bigserial primary key,
    name text,
    gender bigint
);

COPY public.actors (id, name, gender)
    FROM '..\data-preprocess\data\cast_info.csv'
    DELIMITER ','
    CSV HEADER;

create table crew_temp(
    id bigint,
    name text,
    gender bigint
);

COPY public.crew_temp (id, name, gender)
    FROM '..\data-preprocess\data\crew_info.csv'
    DELIMITER ','
    CSV HEADER;

insert into actors (id, name, gender)
select id, name, gender from crew_temp ct where not exists (select * from public.actors a where a.id = ct.id);

drop table crew_temp;

create table acted_in (
    id bigserial,
    actor_id bigint references public.actors(id),
    movie_id bigint references public.movies(id),
    character text
);

COPY public.acted_in (actor_id, movie_id, character)
    FROM '..\data-preprocess\data\cast_movie_relationship.csv'
    DELIMITER ','
    CSV HEADER;

create table crew_movie (
    id bigserial,
    actor_id bigint references public.actors(id),
    movie_id bigint references public.movies(id),
    department text
);

COPY public.crew_movie (actor_id, movie_id, department)
    FROM '..\data-preprocess\data\crew_movie_relationship.csv'
    DELIMITER ','
    CSV HEADER;