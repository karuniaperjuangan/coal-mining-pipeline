SELECT
    m.date AS date,
    w.date 
FROM {{ ref('int_daily_mine_production') }} m
LEFT JOIN {{ ref('int_daily_weather') }} w USING (date)
WHERE w.date IS NULL -- Find mine production date where no respective w.date isnt found