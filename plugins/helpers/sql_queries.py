class SqlQueries:
    # Time as a dimension table
    time_table_insert = ("""
    insert into time(timestamp, day, month, year)
    select distinct "timestamp", extract(day from "timestamp"),extract(month from "timestamp"), extract(year from "timestamp") 
    from staging_stocks
    """)

    # Create the fact table of the star schema
    stock_analysis_table_insert = ("""
    INSERT INTO stock_analysis(time_fk, interest_rates_fk, symbol_fk, news_fk, top_performing_stock_diff)
    SELECT ti.id,
           ir.id,
           sy.id,
           ne.id,
           sta.diff
    FROM
      (SELECT st."timestamp",
              st.diff,
              st.symbol
       FROM staging_stocks st
       INNER JOIN
         (SELECT "timestamp",
                 MAX(diff) diff
          FROM staging_stocks
          GROUP BY "timestamp") tt ON st."timestamp" = tt."timestamp"
       AND st.diff = tt.diff) sta
    LEFT JOIN "time" ti ON ti.timestamp = sta.timestamp
    LEFT JOIN interest_rates ir ON extract(YEAR
                                           FROM sta.timestamp) = ir.year
    AND extract(MONTH
                FROM sta.timestamp) = ir.month
    LEFT JOIN symbols sy ON sy.symbol = sta.symbol
    LEFT JOIN news ne ON ne.timestamp = sta.timestamp
    """)
