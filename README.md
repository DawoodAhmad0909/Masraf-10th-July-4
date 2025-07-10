# Masraf-10th-July-4
## Overview 
## Objectives 

To design a MySQL database system for storing, analyzing, and visualizing real-time and historical forex market data, enabling traders to track currency pairs, trends, and trading performance with efficient querying capabilities

## Creating Database 
``` sql
CREATE DATABASE MD10thJ4_db;
USE MD10thJ4_db;
```
## Creating Tables
### Table:currencies
 sql
CREATE TABLE currencies(
    currency_id     INT PRIMARY KEY AUTO_INCREMENT,
    currency_code   TEXT,
    currency_name   TEXT,
    country         TEXT,
    is_major        BOOLEAN,
    central_bank    TEXT
);

SELECT * FROM currencies ;
```
### Table:exchange_rates
``` sql
CREATE TABLE exchange_rates(
    rate_id               INT PRIMARY KEY AUTO_INCREMENT,
    base_currency_id      INT,
    target_currency_id    INT,
    rate_date             DATE,
    open_rate             DECIMAL(25,10),
    high_rate             DECIMAL(25,10),
    low_rate              DECIMAL(25,10),
    close_rate            DECIMAL(25,10),
    daily_change_pct      DECIMAL(25,10),
    FOREIGN KEY (base_currency_id) REFERENCES currencies(currency_id),
    FOREIGN KEY (target_currency_id) REFERENCES currencies(currency_id)
);

SELECT * FROM exchange_rates ;
```
### Table:economic_events
``` sql
CREATE TABLE economic_events(
    event_id          INT PRIMARY KEY AUTO_INCREMENT,
    currency_id       INT,
    event_date        DATETIME,
    event_name        TEXT,
    impact_level      TEXT,
    actual_value      DECIMAL(20,5),
    forecast_value    DECIMAL(20,5),
    previous_value    DECIMAL(20,5),
    FOREIGN KEY (currency_id) REFERENCES currencies(currency_id)
);

SELECT * FROM economic_events ;
```
### Table:tick_data
``` sql
CREATE TABLE tick_data(
    tick_id              INT PRIMARY KEY AUTO_INCREMENT,
    base_currency_id     INT,
    target_currency_id   INT,
    timestamp            DATETIME,
    bid_price            DECIMAL(20,5),
    ask_price            DECIMAL(20,5),
    volume               INT,
    FOREIGN KEY (base_currency_id) REFERENCES currencies(currency_id),
    FOREIGN KEY (target_currency_id) REFERENCES currencies(currency_id)
);

SELECT * FROM tick_data ;
```
## KEY Queries 

#### 1. List all major currency pairs with their average daily trading range (high-low) for June 2023.
``` sql
SELECT 
    CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
    ROUND(AVG(er.high_rate - er.low_rate), 6) AS avg_daily_range,
    ROUND(MAX(er.high_rate - er.low_rate), 6) AS max_daily_range,
    ROUND(MIN(er.high_rate - er.low_rate), 6) AS min_daily_range
FROM exchange_rates er
JOIN currencies b ON er.base_currency_id = b.currency_id
JOIN currencies t ON er.target_currency_id = t.currency_id
WHERE 
    b.is_major = TRUE AND 
    t.is_major = TRUE AND
    er.rate_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY b.currency_code, t.currency_code
ORDER BY avg_daily_range DESC;
```
#### 2. Show currency pairs that had a positive change for 3+ consecutive days.
``` sql
WITH ranked_changes AS (
    SELECT 
        er.rate_id,er.base_currency_id,er.target_currency_id,
                er.rate_date,er.daily_change_pct,
        CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
        ROW_NUMBER() OVER (PARTITION BY er.base_currency_id, er.target_currency_id ORDER BY er.rate_date) -
        ROW_NUMBER() OVER (
            PARTITION BY er.base_currency_id, er.target_currency_id, 
            CASE WHEN er.daily_change_pct > 0 THEN 1 ELSE 0 END 
            ORDER BY er.rate_date
        ) AS grp
    FROM exchange_rates er
    JOIN currencies b ON er.base_currency_id = b.currency_id
    JOIN currencies t ON er.target_currency_id = t.currency_id
)
SELECT 
    currency_pair,
    MIN(rate_date) AS start_date,
    MAX(rate_date) AS end_date,
    COUNT(*) AS streak_length,
    ROUND(SUM(daily_change_pct), 4) AS total_gain_pct
FROM ranked_changes
WHERE daily_change_pct > 0
GROUP BY currency_pair, grp
HAVING COUNT(*) >= 3
ORDER BY total_gain_pct DESC;
```
#### 3. Find currencies that moved more than 1% in a single day against the USD. 
``` sql   
SELECT 
    CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
    er.rate_date,
    er.daily_change_pct,
    ce.event_name
FROM exchange_rates er
JOIN currencies b ON er.base_currency_id = b.currency_id
JOIN currencies t ON er.target_currency_id = t.currency_id
LEFT JOIN economic_events ce 
    ON DATE(ce.event_date) = er.rate_date
   AND (ce.currency_id = er.base_currency_id OR ce.currency_id = er.target_currency_id)
LEFT JOIN currencies c2 ON ce.currency_id = c2.currency_id
WHERE 
    (b.currency_code = 'USD' OR t.currency_code = 'USD') AND
    ABS(er.daily_change_pct) > 1
ORDER BY er.rate_date, ABS(er.daily_change_pct) DESC;
```
#### 4. Compare exchange rate movements before/after high-impact economic events.
``` sql 
 SELECT 
    ee.event_date,
    CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
    DATE_SUB(DATE(ee.event_date), INTERVAL 1 DAY) AS pre_event_date,
    pre_event.close_rate AS pre_event_close,
    DATE(ee.event_date) AS event_date_only,
    event_day.close_rate AS event_close,
    DATE_ADD(DATE(ee.event_date), INTERVAL 1 DAY) AS post_event_date,
    post_event.close_rate AS post_event_close,
    ROUND(
        ((post_event.close_rate - pre_event.close_rate) / pre_event.close_rate) * 100, 4
    ) AS pct_change
FROM economic_events ee
JOIN currencies ec ON ee.currency_id = ec.currency_id
JOIN exchange_rates pre_event ON (pre_event.rate_date = DATE_SUB(DATE(ee.event_date), INTERVAL 1 DAY))
JOIN exchange_rates event_day ON (event_day.rate_date = DATE(ee.event_date))
JOIN exchange_rates post_event ON (post_event.rate_date = DATE_ADD(DATE(ee.event_date), INTERVAL 1 DAY))
JOIN currencies b ON pre_event.base_currency_id = b.currency_id
JOIN currencies t ON pre_event.target_currency_id = t.currency_id
WHERE 
    (b.currency_id = ee.currency_id OR t.currency_id = ee.currency_id)
    AND event_day.base_currency_id = pre_event.base_currency_id
    AND event_day.target_currency_id = pre_event.target_currency_id
    AND post_event.base_currency_id = pre_event.base_currency_id
    AND post_event.target_currency_id = pre_event.target_currency_id
ORDER BY ee.event_date, currency_pair;
```
#### 5. Identify events where the actual value differed from forecast by more than 20%.
``` sql
SELECT 
    ee.event_name,
    ee.actual_value,
    ee.forecast_value,
    ROUND(
        100 * ABS(ee.actual_value - ee.forecast_value) / NULLIF(ee.forecast_value, 0),
        2
    ) AS deviation_percentage,
    c.currency_code
FROM economic_events ee
JOIN currencies c ON ee.currency_id = c.currency_id
WHERE ABS(ee.actual_value - ee.forecast_value) / NULLIF(ee.forecast_value, 0) > 0.20
ORDER BY deviation_percentage DESC;
```
#### 7. Rank currency pairs by their 10-day historical volatility.
``` sql
WITH daily_changes AS (
    SELECT 
        er.rate_id,
        er.base_currency_id,
        er.target_currency_id,
        er.rate_date,
        CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
        er.daily_change_pct
    FROM exchange_rates er
    JOIN currencies b ON er.base_currency_id = b.currency_id
    JOIN currencies t ON er.target_currency_id = t.currency_id
),
rolling_volatility AS (
    SELECT
        currency_pair,
        rate_date,
        STDDEV_POP(daily_change_pct) OVER (
            PARTITION BY currency_pair 
            ORDER BY rate_date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS volatility_10d
    FROM daily_changes
)
SELECT 
    currency_pair,
    MAX(rate_date) AS last_date,
    ROUND(MAX(volatility_10d), 6) AS latest_10d_volatility
FROM rolling_volatility
GROUP BY currency_pair
HAVING COUNT(*) >= 10
ORDER BY latest_10d_volatility DESC;
```
#### 8. Find days when EUR/USD had unusually high volatility (top 5% of daily ranges).
``` sql
SELECT
    rate_date,
    (high_rate - low_rate) AS daily_range,
    ROUND((ranking / total_count) * 100, 2) AS percentile
FROM (
    SELECT
        rate_date,
        high_rate,
        low_rate,
        @rank := @rank + 1 AS ranking,
        (SELECT COUNT(*) FROM exchange_rates e2 
        WHERE 
                        e2.base_currency_id = e1.base_currency_id 
                        AND e2.target_currency_id = e1.target_currency_id
                ) AS total_count
    FROM exchange_rates e1
    JOIN currencies b ON e1.base_currency_id = b.currency_id
    JOIN currencies t ON e1.target_currency_id = t.currency_id
    CROSS JOIN (SELECT @rank := 0) r
    WHERE b.currency_code = 'EUR' AND t.currency_code = 'USD'
    ORDER BY (high_rate - low_rate) ASC
) ranked_ranges
HAVING percentile >= 95
ORDER BY daily_range DESC;
```
#### 9. Compare Asian vs London vs New York session volatility for major pairs.
``` sql
SELECT 
    CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
    CASE 
        WHEN HOUR(td.timestamp) BETWEEN 0 AND 7 THEN 'Asian'
        WHEN HOUR(td.timestamp) BETWEEN 8 AND 15 THEN 'London'
        WHEN HOUR(td.timestamp) BETWEEN 16 AND 23 THEN 'New York'
        ELSE 'Other'
    END AS session,
    DATE(td.timestamp) AS trading_day,
    ROUND(MAX(td.bid_price) - MIN(td.bid_price), 6) AS session_volatility
FROM tick_data td
JOIN currencies b ON td.base_currency_id = b.currency_id
JOIN currencies t ON td.target_currency_id = t.currency_id
WHERE b.is_major = TRUE AND t.is_major = TRUE
GROUP BY currency_pair, trading_day, session
ORDER BY currency_pair, trading_day, session;
```
#### 10. Show currency pairs with the highest 30-day rolling correlation to EUR/USD.
``` sql
WITH daily_returns AS (
    SELECT 
        rate_date,
        CASE 
            WHEN base_currency_id = 2 AND target_currency_id = 1 THEN 'EUR/USD'
            WHEN base_currency_id = 1 AND target_currency_id = 3 THEN 'USD/JPY'
            WHEN base_currency_id = 4 AND target_currency_id = 1 THEN 'GBP/USD'
            WHEN base_currency_id = 7 AND target_currency_id = 1 THEN 'AUD/USD'
            WHEN base_currency_id = 1 AND target_currency_id = 6 THEN 'USD/CAD'
            WHEN base_currency_id = 2 AND target_currency_id = 3 THEN 'EUR/JPY'
        END AS currency_pair,
        (close_rate / LAG(close_rate, 1) OVER (PARTITION BY base_currency_id, target_currency_id ORDER BY rate_date)) - 1 AS daily_return
    FROM exchange_rates
    WHERE (base_currency_id = 2 AND target_currency_id = 1) 
       OR (base_currency_id = 1 AND target_currency_id = 3) 
       OR (base_currency_id = 4 AND target_currency_id = 1) 
       OR (base_currency_id = 7 AND target_currency_id = 1) 
       OR (base_currency_id = 1 AND target_currency_id = 6) 
       OR (base_currency_id = 2 AND target_currency_id = 3) 
),

eurusd_returns AS (
    SELECT 
        rate_date,
        daily_return AS eurusd_return
    FROM daily_returns
    WHERE currency_pair = 'EUR/USD'
),

pair_returns AS (
    SELECT 
        dr.rate_date,
        dr.currency_pair,
        dr.daily_return AS pair_return,
        er.eurusd_return
    FROM daily_returns dr
    JOIN eurusd_returns er ON dr.rate_date = er.rate_date
    WHERE dr.currency_pair != 'EUR/USD'
),

rolling_stats AS (
    SELECT 
        pr.rate_date,
        pr.currency_pair,
        COUNT(*) AS n,
        SUM(pr.pair_return) AS sum_pair,
        SUM(pr.eurusd_return) AS sum_eurusd,
        SUM(pr.pair_return * pr.eurusd_return) AS sum_product,
        SUM(pr.pair_return * pr.pair_return) AS sum_pair_sq,
        SUM(pr.eurusd_return * pr.eurusd_return) AS sum_eurusd_sq
    FROM pair_returns pr
    JOIN pair_returns pr_window ON pr_window.rate_date BETWEEN DATE_SUB(pr.rate_date, INTERVAL 29 DAY) AND pr.rate_date
                             AND pr_window.currency_pair = pr.currency_pair
    GROUP BY pr.rate_date, pr.currency_pair
    HAVING COUNT(*) >= 20  
),

rolling_correlations AS (
    SELECT
        rate_date,
        currency_pair,
        (n * sum_product - sum_pair * sum_eurusd) / 
        (SQRT(n * sum_pair_sq - sum_pair * sum_pair) * 
         SQRT(n * sum_eurusd_sq - sum_eurusd * sum_eurusd)) AS correlation
    FROM rolling_stats
)

SELECT 
    currency_pair,
    ROUND(AVG(correlation), 4) AS avg_correlation,
    ROUND(MAX(correlation), 4) AS max_correlation,
    ROUND(MIN(correlation), 4) AS min_correlation,
    COUNT(*) AS observation_count
FROM rolling_correlations
GROUP BY currency_pair
ORDER BY ABS(AVG(correlation)) DESC;
```
#### 11. Identify currencies that typically move inversely 
``` sql
WITH daily_returns AS (
    SELECT 
        rate_date,
        CASE 
            WHEN base_currency_id = 2 AND target_currency_id = 1 THEN 'EUR/USD'
            WHEN base_currency_id = 1 AND target_currency_id = 3 THEN 'USD/JPY'
            WHEN base_currency_id = 4 AND target_currency_id = 1 THEN 'GBP/USD'
            WHEN base_currency_id = 7 AND target_currency_id = 1 THEN 'AUD/USD'
            WHEN base_currency_id = 1 AND target_currency_id = 6 THEN 'USD/CAD'
            WHEN base_currency_id = 2 AND target_currency_id = 3 THEN 'EUR/JPY'
            WHEN base_currency_id = 1 AND target_currency_id = 2 THEN 'USD/EUR'
        END AS currency_pair,
        (close_rate / LAG(close_rate, 1) OVER (PARTITION BY base_currency_id, target_currency_id ORDER BY rate_date)) - 1 AS daily_return
    FROM exchange_rates
    WHERE (base_currency_id = 2 AND target_currency_id = 1) 
       OR (base_currency_id = 1 AND target_currency_id = 3) 
       OR (base_currency_id = 4 AND target_currency_id = 1) 
       OR (base_currency_id = 7 AND target_currency_id = 1) 
       OR (base_currency_id = 1 AND target_currency_id = 6) 
       OR (base_currency_id = 2 AND target_currency_id = 3) 
       OR (base_currency_id = 1 AND target_currency_id = 2) 
),

pair_combinations AS (
    SELECT DISTINCT
        a.currency_pair AS pair1,
        b.currency_pair AS pair2
    FROM daily_returns a
    JOIN daily_returns b ON a.currency_pair < b.currency_pair
),

correlation_data AS (
    SELECT
        pc.pair1,
        pc.pair2,
        COUNT(*) AS num_points,
        AVG(dr1.daily_return) AS avg_return1,
        AVG(dr2.daily_return) AS avg_return2,
        SUM((dr1.daily_return - (SELECT AVG(daily_return) FROM daily_returns WHERE currency_pair = pc.pair1)) * 
            (dr2.daily_return - (SELECT AVG(daily_return) FROM daily_returns WHERE currency_pair = pc.pair2))) AS covariance,
        SQRT(SUM(POWER(dr1.daily_return - (SELECT AVG(daily_return) FROM daily_returns WHERE currency_pair = pc.pair1), 2)) * 
             SUM(POWER(dr2.daily_return - (SELECT AVG(daily_return) FROM daily_returns WHERE currency_pair = pc.pair2), 2))) AS denominator
    FROM pair_combinations pc
    JOIN daily_returns dr1 ON pc.pair1 = dr1.currency_pair
    JOIN daily_returns dr2 ON pc.pair2 = dr2.currency_pair AND dr1.rate_date = dr2.rate_date
    GROUP BY pc.pair1, pc.pair2
    HAVING COUNT(*) >= 10
)

SELECT
    pair1,
    pair2,
    num_points AS data_points,
    CASE 
        WHEN denominator = 0 THEN NULL
        ELSE ROUND(covariance / denominator, 4)
    END AS correlation_coefficient,
    CASE 
        WHEN denominator = 0 THEN 'Insufficient data'
        WHEN (covariance / denominator) < -0.7 THEN 'Strong inverse correlation'
        WHEN (covariance / denominator) < -0.5 THEN 'Moderate inverse correlation'
        WHEN (covariance / denominator) < -0.3 THEN 'Weak inverse correlation'
        ELSE 'No significant inverse correlation'
    END AS correlation_strength
FROM correlation_data
WHERE denominator != 0 
AND (covariance / denominator) < -0.3 
ORDER BY correlation_coefficient ASC;
```
#### 12. Calculate average bid-ask spreads by hour of day for EUR/USD.
``` sql
SELECT 
    HOUR(td.timestamp) AS hour_utc,
    ROUND(AVG(td.ask_price - td.bid_price), 6) AS avg_spread,
    SUM(td.volume) AS total_volume
FROM tick_data td
JOIN currencies b ON td.base_currency_id = b.currency_id
JOIN currencies t ON td.target_currency_id = t.currency_id
WHERE b.currency_code = 'EUR' AND t.currency_code = 'USD'
GROUP BY HOUR(td.timestamp)
ORDER BY hour_utc;
```
#### 13. Find the most active trading hours (by volume) for each major currency pair.
``` sql
WITH hourly_volume AS (
    SELECT 
        CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
        HOUR(td.timestamp) AS hour_utc,
        SUM(td.volume) AS total_volume
    FROM tick_data td
    JOIN currencies b ON td.base_currency_id = b.currency_id
    JOIN currencies t ON td.target_currency_id = t.currency_id
    WHERE b.is_major = TRUE AND t.is_major = TRUE
    GROUP BY currency_pair, hour_utc
),
ranked_volume AS (
    SELECT 
        hv.*,
        RANK() OVER (PARTITION BY currency_pair ORDER BY total_volume DESC) AS rnk
    FROM hourly_volume hv
)
SELECT 
    currency_pair,
    hour_utc,
    total_volume
FROM ranked_volume
WHERE rnk = 1
ORDER BY currency_pair;
```
#### 14. Detect days when a currency pair formed a "bullish engulfing" candle pattern.
``` sql
SELECT 
    CONCAT(cb.currency_code, '/', ct.currency_code) AS currency_pair,
    e2.rate_date AS bullish_engulf_date,
    e1.open_rate AS prev_open,
    e1.close_rate AS prev_close,
    e2.open_rate AS curr_open,
    e2.close_rate AS curr_close
FROM exchange_rates e1
JOIN exchange_rates e2 
    ON e1.base_currency_id = e2.base_currency_id 
    AND e1.target_currency_id = e2.target_currency_id
    AND e2.rate_date = DATE_ADD(e1.rate_date, INTERVAL 1 DAY)
JOIN currencies cb ON e2.base_currency_id = cb.currency_id
JOIN currencies ct ON e2.target_currency_id = ct.currency_id
WHERE 
    e1.close_rate < e1.open_rate AND
    e2.open_rate < e1.close_rate AND
    e2.close_rate > e1.open_rate
ORDER BY e2.rate_date, currency_pair;
```
#### 15. Identify support/resistance levels where prices reversed 3+ times.
``` sql
WITH pair_data AS (
    SELECT 
        er.base_currency_id,
        er.target_currency_id,
        er.rate_date,
        er.open_rate,
        er.close_rate,
        ROUND(er.close_rate, 3) AS rounded_close,
        CASE 
            WHEN er.close_rate > er.open_rate THEN 'up'
            WHEN er.close_rate < er.open_rate THEN 'down'
            ELSE 'flat'
        END AS direction
    FROM exchange_rates er
),
reversal_points AS (
    SELECT 
        p1.base_currency_id,
        p1.target_currency_id,
        p2.rate_date,
        p1.rounded_close AS level,
        p1.direction AS dir1,
        p2.direction AS dir2
    FROM pair_data p1
    JOIN pair_data p2 
        ON p1.base_currency_id = p2.base_currency_id 
        AND p1.target_currency_id = p2.target_currency_id 
        AND p2.rate_date = DATE_ADD(p1.rate_date, INTERVAL 1 DAY)
        AND ABS(p1.rounded_close - p2.rounded_close) <= 0.002  
    WHERE p1.direction != p2.direction
),
level_counts AS (
    SELECT 
        CONCAT(b.currency_code, '/', t.currency_code) AS currency_pair,
        level,
        COUNT(*) AS reversal_count
    FROM reversal_points r
    JOIN currencies b ON r.base_currency_id = b.currency_id
    JOIN currencies t ON r.target_currency_id = t.currency_id
    GROUP BY currency_pair, level
)
SELECT 
    currency_pair,
    level,
    reversal_count
FROM level_counts
WHERE reversal_count >= 3
ORDER BY reversal_count DESC;
```
#### 16. Find potential triangular arbitrage opportunities in the tick data.
``` sql
WITH currency_triples AS (
    SELECT 
        a.base_currency_id AS currency1,
        a.target_currency_id AS currency2,
        b.target_currency_id AS currency3
    FROM tick_data a
    JOIN tick_data b ON a.target_currency_id = b.base_currency_id
    WHERE a.base_currency_id <> b.target_currency_id
    GROUP BY 1, 2, 3
),
arbitrage_opportunities AS (
    SELECT 
        t1.timestamp,
        c1.currency_code AS currency1_code,
        c2.currency_code AS currency2_code,
        c3.currency_code AS currency3_code,
        t1.ask_price AS leg1_ask,  
        t2.bid_price AS leg2_bid,   
        t3.bid_price AS leg3_bid,  

        ((1 / t1.ask_price) * t2.bid_price * t3.bid_price - 1) * 100 AS profit_pct,
        TIMESTAMPDIFF(SECOND, t1.timestamp, 
                      LEAST(
                          COALESCE((SELECT MIN(timestamp) FROM tick_data 
                                   WHERE timestamp > t1.timestamp AND 
                                         base_currency_id = t1.base_currency_id AND 
                                         target_currency_id = t1.target_currency_id), 
                                  t1.timestamp + INTERVAL 1 HOUR),
                          COALESCE((SELECT MIN(timestamp) FROM tick_data 
                                   WHERE timestamp > t1.timestamp AND 
                                         base_currency_id = t2.base_currency_id AND 
                                         target_currency_id = t2.target_currency_id), 
                                  t1.timestamp + INTERVAL 1 HOUR),
                          COALESCE((SELECT MIN(timestamp) FROM tick_data 
                                   WHERE timestamp > t1.timestamp AND 
                                         base_currency_id = t3.base_currency_id AND 
                                         target_currency_id = t3.target_currency_id), 
                                  t1.timestamp + INTERVAL 1 HOUR)
                      )) AS opportunity_window_seconds
    FROM tick_data t1
    JOIN tick_data t2 ON t1.target_currency_id = t2.base_currency_id 
                     AND t2.timestamp = (
                         SELECT MIN(timestamp) 
                         FROM tick_data 
                         WHERE timestamp >= t1.timestamp 
                         AND base_currency_id = t2.base_currency_id 
                         AND target_currency_id = t2.target_currency_id
                     )
    JOIN tick_data t3 ON t2.target_currency_id = t3.target_currency_id 
                     AND t3.base_currency_id = t1.base_currency_id
                     AND t3.timestamp = (
                         SELECT MIN(timestamp) 
                         FROM tick_data 
                         WHERE timestamp >= t2.timestamp 
                         AND base_currency_id = t3.base_currency_id 
                         AND target_currency_id = t3.target_currency_id
                     )
    JOIN currencies c1 ON t1.base_currency_id = c1.currency_id
    JOIN currencies c2 ON t1.target_currency_id = c2.currency_id
    JOIN currencies c3 ON t2.target_currency_id = c3.currency_id
    WHERE ((1 / t1.ask_price) * t2.bid_price * t3.bid_price) > 1.0001 
)
SELECT 
    timestamp,
    CONCAT(currency1_code, '/', currency2_code, ' → ',
    currency2_code, '/', currency3_code, ' → ', 
    currency3_code, '/', currency1_code) AS arbitrage_path,
    profit_pct,
    opportunity_window_seconds,
    profit_pct AS max_profit_pct
FROM arbitrage_opportunities
ORDER BY profit_pct DESC
LIMIT 10;
```
#### 17. Calculate how long arbitrage opportunities typically last.
``` sql
SELECT 
    c1.currency_code AS currency1,
    c2.currency_code AS currency2,
    c3.currency_code AS currency3,
    MIN(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15)*15)) AS start_time,
    MAX(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15)*15)) AS end_time,
    TIMESTAMPDIFF(SECOND, 
        MIN(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15)*15)), 
        MAX(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15)*15))
    ) + 15 AS duration_seconds,
    COUNT(DISTINCT FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15)) AS interval_count
FROM tick_data t1
JOIN tick_data t2 
    ON FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15) = FLOOR(UNIX_TIMESTAMP(t2.timestamp)/15)
    AND t1.target_currency_id = t2.base_currency_id
JOIN tick_data t3 
    ON FLOOR(UNIX_TIMESTAMP(t1.timestamp)/15) = FLOOR(UNIX_TIMESTAMP(t3.timestamp)/15)
    AND t2.target_currency_id = t3.base_currency_id
    AND t3.target_currency_id = t1.base_currency_id
JOIN currencies c1 ON t1.base_currency_id = c1.currency_id
JOIN currencies c2 ON t1.target_currency_id = c2.currency_id
JOIN currencies c3 ON t2.target_currency_id = c3.currency_id
WHERE (t1.bid_price * t2.bid_price * t3.bid_price) > 1.0005
  AND t1.base_currency_id = LEAST(t1.base_currency_id, t1.target_currency_id, t2.target_currency_id)
GROUP BY t1.base_currency_id, t1.target_currency_id, t2.target_currency_id
HAVING interval_count > 1
ORDER BY duration_seconds DESC;
```
#### 20. Identify currencies that consistently strengthened/weakened over a 30-day period.
``` sql
WITH recent_rates AS (
    SELECT 
        b.currency_id,
        b.currency_code AS base_currency,
        t.currency_code AS target_currency,
        er.rate_date,
        er.close_rate,
        LAG(er.close_rate) OVER (PARTITION BY er.base_currency_id ORDER BY er.rate_date) AS prev_close_rate
    FROM exchange_rates er
    JOIN currencies b ON er.base_currency_id = b.currency_id
    JOIN currencies t ON er.target_currency_id = t.currency_id
    WHERE t.currency_code = 'USD' 
      AND er.rate_date >= DATE_SUB('2023-06-30', INTERVAL 30 DAY) -- Change date to find data before the date
      AND b.is_major = TRUE
),
daily_trends AS (
    SELECT 
        base_currency,
        rate_date,
        CASE 
            WHEN close_rate > prev_close_rate THEN 1
            WHEN close_rate < prev_close_rate THEN -1
            ELSE 0
        END AS day_trend
    FROM recent_rates
    WHERE prev_close_rate IS NOT NULL
),
trend_summary AS (
    SELECT 
        base_currency,
        SUM(day_trend = 1) AS up_days,
        SUM(day_trend = -1) AS down_days,
        COUNT(*) AS total_days
    FROM daily_trends
    GROUP BY base_currency
)
SELECT 
    base_currency,
    up_days,
    down_days,
    total_days,
    CASE
        WHEN up_days = total_days THEN 'Consistently Strengthened'
        WHEN down_days = total_days THEN 'Consistently Weakened'
        ELSE 'Mixed'
    END AS trend_status
FROM trend_summary
WHERE up_days = total_days OR down_days = total_days
ORDER BY base_currency;
```
## Conclusion 
