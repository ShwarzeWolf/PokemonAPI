CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS golden;

CREATE TABLE IF NOT EXISTS golden.types_statistics (
    "Pokemon Type" TEXT PRIMARY KEY,
    "Number of Pokemons" INT,
    "Delta from next rank" INT,
    "Delta from previous rank" INT
);

CREATE TABLE IF NOT EXISTS golden.moves_statistics (
    "Move Name" TEXT PRIMARY KEY,
    "Attack Usage" INT,
    "Delta from next rank" INT,
    "Delta from previous rank" INT
);

CREATE TABLE IF NOT EXISTS golden.stats_statistics (
    "Pokemon" TEXT PRIMARY KEY,
    "Total Power" INT
);

CREATE TABLE IF NOT EXISTS golden.types_generations_statistics (
    "Pokemon Type" TEXT PRIMARY KEY,
    "I" INT DEFAULT 0,
    "II" INT DEFAULT 0,
    "III" INT DEFAULT 0,
    "IV" INT DEFAULT 0,
    "V" INT DEFAULT 0,
    "VI" INT DEFAULT 0,
    "VII" INT DEFAULT 0,
    "VIII" INT DEFAULT 0
);

-- Function to refresh types_statistics table
CREATE OR REPLACE FUNCTION golden.refresh_types_statistics() RETURNS VOID AS $$
BEGIN
    DELETE FROM golden.types_statistics;
    INSERT INTO golden.types_statistics
    SELECT
        pt."type" AS "Pokemon Type",
        COUNT(*) AS "Number of Pokemons",
        ABS(COUNT(*) - LEAD(COUNT(*)) OVER (ORDER BY COUNT(*) DESC, pt."type")) AS "Delta from next rank",
        ABS(COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY COUNT(*) DESC, pt."type")) AS "Delta from previous rank"
    FROM silver.pokemon_types pt
    GROUP BY pt."type"
    ORDER BY "Number of Pokemons" DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh moves_statistics table
CREATE OR REPLACE FUNCTION golden.refresh_moves_statistics() RETURNS VOID AS $$
BEGIN
    DELETE FROM golden.moves_statistics;
    INSERT INTO golden.moves_statistics
    SELECT
        pm.move AS "Move Name",
        COUNT(*) AS "Attack Usage",
        ABS(COUNT(*) - LEAD(COUNT(*)) OVER (ORDER BY COUNT(*) DESC, pm.move)) AS "Delta from next rank",
        ABS(COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY COUNT(*) DESC, pm.move)) AS "Delta from previous rank"
    FROM silver.pokemon_moves pm
    GROUP BY pm.move
    ORDER BY "Attack Usage" DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh stats_statistics table
CREATE OR REPLACE FUNCTION golden.refresh_stats_statistics() RETURNS VOID AS $$
BEGIN
    DELETE FROM golden.stats_statistics;
    INSERT INTO golden.stats_statistics
    SELECT
        ps.pokemon AS "Pokemon",
        SUM(ps.power) AS "Total Power"
    FROM silver.pokemon_stats ps
    GROUP BY ps.pokemon
    ORDER BY "Total Power" DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh types_generations_statistics table
CREATE OR REPLACE FUNCTION golden.refresh_types_generations_statistics() RETURNS VOID AS $$
BEGIN
    DELETE FROM golden.types_generations_statistics;
    INSERT INTO golden.types_generations_statistics
    SELECT
        pt."type" AS "Pokemon Type",
        COUNT(CASE WHEN gs.generation = 'generation-i' THEN pt.pokemon END) AS "I",
        COUNT(CASE WHEN gs.generation = 'generation-ii' THEN pt.pokemon END) AS "II",
        COUNT(CASE WHEN gs.generation = 'generation-iii' THEN pt.pokemon END) AS "III",
        COUNT(CASE WHEN gs.generation = 'generation-iv' THEN pt.pokemon END) AS "IV",
        COUNT(CASE WHEN gs.generation = 'generation-v' THEN pt.pokemon END) AS "V",
        COUNT(CASE WHEN gs.generation = 'generation-vi' THEN pt.pokemon END) AS "VI",
        COUNT(CASE WHEN gs.generation = 'generation-vii' THEN pt.pokemon END) AS "VII"
    FROM silver.pokemon_types pt
    INNER JOIN silver.pokemon_species ps ON pt.pokemon = ps.pokemon
    INNER JOIN silver.generation_species gs ON ps.specie_id = gs.specie_id
    GROUP BY pt."type"
    ORDER BY "Pokemon Type";
END;
$$ LANGUAGE plpgsql;
