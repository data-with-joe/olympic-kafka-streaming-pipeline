CREATE VIEW olympics_by_year AS
SELECT
    "Year",
    COUNT(DISTINCT "Games") AS olympic_games,
    COUNT(*) AS athlete_events
FROM olympic_events
GROUP BY "Year"
ORDER BY "Year";