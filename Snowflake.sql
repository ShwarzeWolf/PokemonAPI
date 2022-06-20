create database pokemons;

create or replace stage staging
  url='s3://de-school-snowflake/snowpipe/Sharaeva/'
  credentials=(aws_key_id='aws_key_id' aws_secret_key='aws_secret_key');

list @staging;

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  trim_space = false skip_header=1
  error_on_column_count_mismatch = false escape = 'none' 
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for csv files';
  
create or replace table pokemon_stats (
    power string,
    effort string,
    characteristic string,
    stat_url string,
    pokemon_name string );
    
-- change from here
    
create or replace table pokemon_generations (
    pokemon_name string,
    species_url string,
    species_generation string,
    specie_id integer);
    
create or replace table pokemon_moves (
    pokemon_name string,
    pokemon_url string,
    move_name string,
    pokemon_id integer);
    
create or replace table pokemon_types (
    pokemon_name string,
    pokemon_type string,
    pokemon_id integer);
  
create or replace table pokemon_species (
    is_default boolean,
    pokemon_name string,
    pokemon_url string,
    speci_id integer);
    
copy into pokemon_stats from @staging file_format=csv PATTERN = 'pokemon_stats.csv';

copy into pokemon_generations from @staging file_format=csv PATTERN = 'pokemon_generations.csv';

copy into pokemon_moves from @staging file_format=csv PATTERN = 'pokemon_moves.csv';

copy into pokemon_types from @staging file_format=csv PATTERN = 'pokemon_types.csv';

copy into pokemon_species from @staging file_format=csv PATTERN = 'species_pokemons.csv';


create schema data_marts;
use schema data_marts;

-- Сколько покемонов в каждом типе (type в терминах API), насколько это меньше чем у следующего по рангу типа? А насколько больше, чем у предыдущего?~~

create or replace view "Types statistcs" as (
select 
    pokemon_type as "Pokemon Type", 
    count(*) as "Number of pokemons", 
    lead("Number of pokemons") over (order by "Number of pokemons") - "Number of pokemons" as "Delta from next rank", 
    "Number of pokemons" - lag("Number of pokemons") over (order by "Number of pokemons") as "Delta from previous rank"
from public.pokemon_types
group by pokemon_type
order by "Number of pokemons" desc);

-- Сколько у разных атак (moves в терминах API) использующих их покемонов? + показать дельту от следующей и предыдущей по популярности атаки. 
create or replace view "Moves statistics" as
select 
    move_name as "Move name",
    count(*) as "Attack usage", 
    lead("Attack usage") over (order by "Attack usage") - "Attack usage" as "Delta from next rank", 
    "Attack usage" - lag("Attack usage") over (order by "Attack usage") as "Delta from previous rank"
from public.pokemon_moves
group by move_name
order by "Attack usage" desc;

-- Составить рейтинг покемонов по сумме их характеристик (stats в терминах API). Например, если у покемона есть только 2 статы: HP 20 & attack 25, то в финальный рейтинг идёт сумма характеристик: 20 + 25 = 45.
create or replace view "Stats statistics" as
select 
    pokemon_name as "Pokemon",
    sum(power) as "Total power" 
from public.pokemon_stats
group by pokemon_name
order by "Total power"desc;

-- Показать количество покемонов по типам (строки таблицы, type в терминах API) и поколениям (колонки таблицы, generations в терминах API).
create or replace view "Types Generations statistic" as (
with types_generations_statistics as (
    select 
        pokemon_type,
        species_generation, 
        t.pokemon_name
    from public.pokemon_types t
    inner join public.pokemon_species ps on t.pokemon_name = ps.pokemon_name
    inner join public.pokemon_generations g on ps.speci_id = g.specie_id
    group by pokemon_type, species_generation, t.pokemon_name
) select * 
from types_generations_statistics
pivot(count(pokemon_name) for species_generation in ('generation-i', 'generation-ii', 'generation-iii', 'generation-iv', 'generation-v', 'generation-vi', 'generation-vii', 'generation-viii'))
      as p
order by pokemon_type )


select * from "Types Generations statistic"