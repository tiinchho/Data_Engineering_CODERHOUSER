CREATE TABLE IF NOT EXISTS martin_pm_coderhouse.exchange_stocks_data
(
	aud_ins_dttm TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,trading_volume NUMERIC(18,0)   ENCODE az64
	,volume_weighted_average_price NUMERIC(18,0)   ENCODE az64
	,open_price NUMERIC(18,0)   ENCODE az64
	,close_price NUMERIC(18,0)   ENCODE az64
	,highest_price NUMERIC(18,0)   ENCODE az64
	,lowest_price NUMERIC(18,0)   ENCODE az64
	,event_datetime TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,number_of_transactions NUMERIC(18,0)   ENCODE az64
	,exchange_symbol VARCHAR(255)   ENCODE lzo
)