CREATE TABLE IF NOT EXISTS martin_pm_coderhouse.exchange_stocks_data
(
	aud_ins_dttm TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_symbol VARCHAR(256)   ENCODE lzo
	,event_date DATE   ENCODE az64
	,trading_volume BIGINT   ENCODE az64
	,volume_weighted_average_price DOUBLE PRECISION   ENCODE RAW
	,open_price DOUBLE PRECISION   ENCODE RAW
	,close_price DOUBLE PRECISION   ENCODE RAW
	,highest_price DOUBLE PRECISION   ENCODE RAW
	,lowest_price DOUBLE PRECISION   ENCODE RAW
	,number_of_transactions BIGINT   ENCODE az64
)
;