DROP TABLE IF EXISTS sdata.sse_idx_quote;
CREATE TABLE sdata.sse_idx_quote
(
	MDStreamID         CHAR(5),
	SecurityID         CHAR(6),
	Symbol             CHAR(8),
	TradeVolume        DECIMAL(16),
	TotalValueTraded   DECIMAL(16,2),
	PreClosePx         DECIMAL(11,4),
	OpenPrice          DECIMAL(11,4),
	HighPrice          DECIMAL(11,4),
	LowPrice           DECIMAL(11,4),
	TradePrice         DECIMAL(11,4),
	ClosePx            DECIMAL(11,4),
	TradingPhaseCode   CHAR(8),
	TS          CHAR(12)
) PARTITIONED BY (TRAD_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
;

DROP TABLE IF EXISTS sdata.sse_sec_quote;
CREATE TABLE sdata.sse_sec_quote
(
	MDStreamID         CHAR(5),
	SecurityID         CHAR(6),
	Symbol             CHAR(8),
	TradeVolume        DECIMAL(16),
	TotalValueTraded   DECIMAL(16,2),
	PreClosePx         DECIMAL(11,3),
	OpenPrice          DECIMAL(11,3),
	HighPrice          DECIMAL(11,3),
	LowPrice           DECIMAL(11,3),
	TradePrice         DECIMAL(11,3),
	ClosePx            DECIMAL(11,3),
	BuyPrice1          DECIMAL(11,3),
	BuyVolume1         DECIMAL(12),
	SellPrice1         DECIMAL(11,3),
	SellVolume1        DECIMAL(12),
	BuyPrice2          DECIMAL(11,3),
	BuyVolume2         DECIMAL(12),
	SellPrice2         DECIMAL(11,3),
	SellVolume2        DECIMAL(12),
	BuyPrice3          DECIMAL(11,3),
	BuyVolume3         DECIMAL(12),
	SellPrice3         DECIMAL(11,3),
	SellVolume3        DECIMAL(12),
	BuyPrice4          DECIMAL(11,3),
	BuyVolume4         DECIMAL(12),
	SellPrice4         DECIMAL(11,3),
	SellVolume4        DECIMAL(12),
	BuyPrice5          DECIMAL(11,3),
	BuyVolume5         DECIMAL(12),
	SellPrice5         DECIMAL(11,3),
	SellVolume5        DECIMAL(12),
	PreCloseIOPV       DECIMAL(11,3),
	IOPV               DECIMAL(11,3),
	TradingPhaseCode   CHAR(8),
	TS          CHAR(12)
) PARTITIONED BY (TRAD_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
;


DROP TABLE IF EXISTS sdata.szse_sec_quote;
CREATE TABLE sdata.szse_sec_quote
(
	HQZQDM	CHAR(6),
	HQZQJC	CHAR(8),
	HQZRSP	DECIMAL(9,3),
	HQJRKP	DECIMAL(9,3),
	HQZJCJ	DECIMAL(9,3),
	HQCJSL	DECIMAL(12,0),
	HQCJJE	DECIMAL(17,3),
	HQCJBS	DECIMAL(9,0),
	HQZGCJ	DECIMAL(9,3),
	HQZDCJ	DECIMAL(9,3),
	HQSYL1	DECIMAL(7,2),
	HQSYL2	DECIMAL(7,2),
	HQJSD1	DECIMAL(9,3),
	HQJSD2	DECIMAL(9,3),
	HQHYCC	DECIMAL(12,0),
	HQSJW5	DECIMAL(9,3),
	HQSSL5	DECIMAL(12,0),
	HQSJW4	DECIMAL(9,3),
	HQSSL4	DECIMAL(12,0),
	HQSJW3	DECIMAL(9,3),
	HQSSL3	DECIMAL(12,0),
	HQSJW2	DECIMAL(9,3),
	HQSSL2	DECIMAL(12,0),
	HQSJW1	DECIMAL(9,3),
	HQSSL1	DECIMAL(12,0),
	HQBJW1	DECIMAL(9,3),
	HQBSL1	DECIMAL(12,0),
	HQBJW2	DECIMAL(9,3),
	HQBSL2	DECIMAL(12,0),
	HQBJW3	DECIMAL(9,3),
	HQBSL3	DECIMAL(12,0),
	HQBJW4	DECIMAL(9,3),
	HQBSL4	DECIMAL(12,0),
	HQBJW5	DECIMAL(9,3),
	HQBSL5	DECIMAL(12,0),
	TS          CHAR(12)
) PARTITIONED BY (TRAD_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
;

