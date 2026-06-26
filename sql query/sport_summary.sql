CREATE VIEW sport_summary AS
SELECT
    "Sport",
    COUNT(*) AS total_events,
    COUNT("Medal") AS total_medals
FROM olympic_events
GROUP BY "Sport";