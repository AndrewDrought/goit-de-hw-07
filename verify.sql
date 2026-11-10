SELECT medal, COUNT(*) AS source_count
FROM olympic_dataset.athlete_event_results
WHERE medal IN ('Bronze', 'Silver', 'Gold')
GROUP BY medal
ORDER BY medal;

SELECT id, medal_type, `count`, created_at
FROM zasukha_medal_counts
ORDER BY id;
