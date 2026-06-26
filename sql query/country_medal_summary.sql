SELECT *
FROM olympic_events
limit 20 ;CREATE VIEW country_medal_summary AS
SELECT
    "NOC",
    COUNT(*) FILTER (WHERE "Medal" = 'GOLD') AS gold,
    COUNT(*) FILTER (WHERE "Medal" = 'SILVER') AS silver,
    COUNT(*) FILTER (WHERE "Medal" = 'BRONZE') AS bronze,
    COUNT("Medal") AS total_medals
FROM olympic_events
WHERE "Medal" IS NOT NULL
GROUP BY "NOC"
ORDER BY total_medals DESC;
