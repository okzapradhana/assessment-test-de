SELECT
    COUNT(*)
FROM `flights.keyword_searches_all_time`
WHERE FORMAT_DATE('%Y-%m', DATE(search_at)) = '{{ data_interval_start.format("YYYY-MM") }}'