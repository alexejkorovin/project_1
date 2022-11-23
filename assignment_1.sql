-----------------Schema-----------------------
DROP TABLE IF EXISTS Person CASCADE;
CREATE TABLE Person (
 	login_name	VARCHAR(32)		CHECK(length(login_name) >= 6),
	password	VARCHAR(20)		CHECK(length(password) >= 8)		NOT NULL,
	email 		VARCHAR(254)	NOT NULL,
	full_name	VARCHAR(20)		NOT NULL,
	address		VARCHAR(50),
	PRIMARY KEY (login_name)
);

DROP TABLE IF EXISTS Customer CASCADE;
CREATE TABLE Customer (
	cash_balance	INTEGER 	DEFAULT 0 NOT NULL, 	
	mobile_number	VARCHAR(15) NOT NULL,
	PRIMARY KEY (login_name)
) inherits(Person);

DROP TABLE IF EXISTS Administrator CASCADE;
CREATE TABLE Administrator (
	remuneration	INTEGER		DEFAULT 0 NOT NULL,
	PRIMARY KEY (login_name)
) inherits(Person);

DROP TABLE IF EXISTS Qualification CASCADE;
CREATE TABLE Qualification(
	qualification_id	INTEGER		NOT NULL, 
	level 			VARCHAR(20) DEFAULT '' NOT NULL,
	login_name		VARCHAR(32) CHECK(length(login_name)>=6),
	PRIMARY KEY (qualification_id), 
	FOREIGN KEY (login_name) REFERENCES Administrator(login_name)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

DROP TABLE IF EXISTS Holding CASCADE;
CREATE TABLE Holding (
	num_of_etf		INTEGER NOT NULL,
	login_name		VARCHAR(32) REFERENCES Customer(login_name)
	ON DELETE CASCADE
    ON UPDATE CASCADE
    CHECK(length(login_name) >= 6)
    NOT NULL,
  ETF_code VARCHAR(6) REFERENCES ETF(ETF_Code)
	ON DELETE CASCADE
	ON UPDATE CASCADE
	NOT NULL CHECK(length(etf_code) >= 3),
  PRIMARY KEY (login_name, ETF_code)
);

DROP TABLE IF EXISTS ETF CASCADE;
CREATE TABLE ETF (
	etf_code		VARCHAR(6) CHECK(length(etf_code) >= 3) PRIMARY KEY,
	etf_name		VARCHAR(50) NOT NULL,
	minimum_invest 	INTEGER CHECK(minimum_invest >= 500) NOT NULL,
	CATEGORY 		VARCHAR(20) NOT NULL,
	date_established	DATE NOT NULL,
	description		VARCHAR(50) NOT NULL
);

DROP TABLE IF EXISTS CLOSING_PRICE_HISTORY CASCADE;
CREATE TABLE CLOSING_PRICE_HISTORY(
	price_history_id	SERIAL,
	closing_price		INTEGER NOT NULL,
	date 				DATE NOT NULL,
	units_outstanding	INTEGER,
	etf_code 			VARCHAR(6) REFERENCES ETF(etf_code) 
	ON DELETE CASCADE
	ON UPDATE CASCADE
	CHECK(length(etf_code) >= 3) 
	NOT NULL,
	PRIMARY KEY (ETF_code, price_history_id)
);	

DROP TABLE IF EXISTS Deposit CASCADE;
CREATE TABLE Deposit (
	transaction_id	SERIAL PRIMARY KEY,
	trade_date		DATE NOT NULL,
	amount			INTEGER NOT NULL CHECK(amount > 0),
	login_name		VARCHAR(32) REFERENCES Customer(login_name)
	ON DELETE CASCADE
	ON UPDATE CASCADE
	NOT NULL 
	CHECK(length(login_name) >= 6)
);

DROP TABLE IF EXISTS Trade CASCADE;
CREATE TABLE Trade (
	  transaction_id	SERIAL PRIMARY KEY,
	  trade_date	DATE NOT NULL,
	  final_amount		INTEGER DEFAULT 0,
	  login_name		VARCHAR(32) REFERENCES Customer(login_name)
	  ON DELETE CASCADE
	  ON UPDATE CASCADE
	  NOT NULL CHECK(length(login_name) >= 6),
	  num_of_ETF		INTEGER CHECK(num_of_ETF > 0) NOT NULL,
	  price_per_ETF		INTEGER NOT NULL,
	  trade_type		VARCHAR(4) CHECK(trade_type in ('BUY', 'SELL')) NOT NULL,
	  etf_code			VARCHAR(6) REFERENCES ETF(etf_code) 
	  ON DELETE CASCADE
	  ON UPDATE CASCADE
	  CHECK(length(etf_code) >= 3)
	  NOT NULL
);

DROP TABLE IF EXISTS Regular_Invest CASCADE;
CREATE TABLE Regular_Invest (
	transaction_id	SERIAL PRIMARY KEY,
	login_name		VARCHAR(32) REFERENCES Customer(login_name)
	ON DELETE CASCADE
	ON UPDATE CASCADE
	NOT NULL
	CHECK(length(login_name) >= 6),
	num_of_ETF		INTEGER CHECK(num_of_ETF > 0) NOT NULL,
	price_per_ETF	INTEGER NOT NULL,
	etf_code		VARCHAR(6) REFERENCES ETF(etf_code) 
	ON DELETE CASCADE
	ON UPDATE CASCADE
	CHECK(length(etf_code) >= 3)
	NOT NULL,
	start_date		DATE NOT NULL,
	frequency		VARCHAR(10) CHECK(frequency in ('FORNIGHTLY', 'MONTHLY')) NOT NULL,
	end_date		DATE DEFAULT(CURRENT_DATE + INTERVAL '12 months')
);

------------Triggers------------------------
CREATE OR REPLACE FUNCTION TR_Trade_BeforeInsert_function()
  RETURNS TRIGGER
AS $$
BEGIN
  -- Check minimum investment
  IF NEW.trade_type = 'BUY' AND NEW.num_of_ETF * NEW.price_per_ETF < (SELECT minimum_invest FROM ETF WHERE etf_code = NEW.etf_code)
  THEN
    RAISE EXCEPTION 'Minimum Investment for ETF not met';
  END IF;

  -- Pre-check 1: For BUY, check if customer has enough money for the trade
  IF NEW.trade_type = 'BUY'
    AND (SELECT cash_balance FROM Customer WHERE login_name = NEW.login_name) < NEW.num_of_ETF * NEW.price_per_ETF
  THEN
    RAISE EXCEPTION 'Cash Balance not enough for the Trade';
  END IF;

  -- Pre-check 2: For SELL, check if the customer has enough units of ETF to sell
  IF NEW.trade_type = 'SELL'
    AND (SELECT COALESCE(num_of_ETF, 0) FROM Holding WHERE ETF_code = NEW.ETF_Code AND login_name = NEW.login_name) < NEW.num_of_ETF
  THEN
    RAISE EXCEPTION 'Not enough unit of ETF to sell';
  END IF;
  
  -- Update Cash_Balance in Customer based on the trade_type
  WITH brokerage AS (
    SELECT
      CASE
        WHEN 0.02 * NEW.num_of_ETF * NEW.price_per_ETF < 2 THEN 2
        WHEN 0.02 * NEW.num_of_ETF * NEW.price_per_ETF > 50 THEN 50
        ELSE 0.02 * NEW.num_of_ETF * NEW.price_per_ETF
      END AS amount
  )
  UPDATE Customer
  SET cash_balance = 
    CASE 
      WHEN NEW.trade_type = 'BUY' THEN cash_balance - NEW.num_of_ETF * NEW.price_per_ETF - (SELECT amount FROM brokerage)
      WHEN NEW.trade_type = 'SELL' THEN cash_balance + NEW.num_of_ETF * NEW.price_per_ETF - (SELECT amount FROM brokerage)
    END
  WHERE login_name = NEW.login_name;

  -- Update num_of_holding in Holding
  ---- Case 1: Holding not existed
  IF NOT EXISTS (
    SELECT * 
    FROM Holding
    WHERE Holding.ETF_code = NEW.ETF_code and Holding.login_name = NEW.login_name
  ) 
  THEN 
    INSERT INTO Holding(num_of_ETF, login_name, ETF_code)
    VALUES (
      NEW.num_of_ETF,
      NEW.login_name,
      NEW.ETF_code
    );
  ELSE
    ---- Case 2: Holding existed
    UPDATE Holding
    SET num_of_ETF = 
      CASE
        WHEN NEW.trade_type = 'BUY' THEN num_of_ETF + NEW.num_of_ETF
        WHEN NEW.trade_type = 'SELL' THEN num_of_ETF - NEW.num_of_ETF
      END
    WHERE ETF_code = NEW.ETF_code and  login_name = NEW.login_name;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS TR_Trade_BeforeInsert ON Trade;
CREATE TRIGGER TR_Trade_BeforeInsert
BEFORE INSERT ON Trade 
FOR EACH ROW
EXECUTE PROCEDURE TR_Trade_BeforeInsert_function();


CREATE OR REPLACE FUNCTION TR_Trade_AfterInsert_function()
  RETURNS TRIGGER
AS $$
BEGIN
  UPDATE Trade
  SET trade_date = trade_date + INTERVAL '2 days'
  WHERE transaction_id = NEW.transaction_id;
  RETURN NEW;
END;$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS TR_Trade_AfterInsert ON Trade;
CREATE TRIGGER TR_Trade_AfterInsert
AFTER INSERT ON Trade 
FOR EACH ROW
EXECUTE PROCEDURE TR_Trade_AfterInsert_function();


----
--after insert on deposit uprdate Customer's cash balace
CREATE OR REPLACE FUNCTION upd_cash_amount_bal()
RETURNS trigger AS $$
BEGIN
  
		UPDATE Customer SET cash_balance = cash_balance + NEW.amount
		WHERE login_name = NEW.login_name; 
							
RETURN NEW;
END;$$ LANGUAGE 'plpgsql';
BEGIN;
DROP TRIGGER IF EXISTS upd_cash_amount ON Deposit;
CREATE TRIGGER upd_cash_amount
AFTER INSERT ON Deposit
FOR EACH ROW
EXECUTE PROCEDURE upd_cash_amount_bal();
COMMIT;


----------------Inserts---------
INSERT INTO PERSON(
	login_name,
	password,
	email,
	full_name,
	address) VALUES(
	'admin-111',
	'123456789',
	'admin@gmail.com',
	'Best Admin',
	'Sydney'
);

INSERT INTO Administrator VALUES (
  'admin-111',
  '123456789',
  'admin@gmail.com',
  'Best Admin',
  'Sydney',
  9999
);

INSERT INTO Qualification VALUES (1111, 'level 1', 'admin-111');


INSERT INTO Customer(
  login_name,
  password,
  email,
  full_name,
  address,
  mobile_number,
  cash_balance
) VALUES (
  'customer-111',
  '123456789',
  'customer-111@gmail.com',
  'Best Customer',
  'Sydney',
  '0400000000',
  100000000
);


INSERT INTO ETF (
  etf_code,
  etf_name,
  minimum_invest,
  category,
  date_established,
  description 
) VALUES (
  'ETF-1',
  'Number One ETF',
  500,
  'Tech',
  '2010-10-10',
  'This is the number one ETF'
);

INSERT INTO TRADE(
  transaction_id,
  trade_date,
  login_name,
  num_of_ETF,
  price_per_ETF,
  trade_type,
  etf_code
) VALUES (
  DEFAULT,
  '2022-09-21',
  'customer-111',
  1000,
  50,
  'BUY',
  'ETF-1'
);


INSERT INTO Customer(
  login_name,
  password,
  email,
  full_name,
  address,
  mobile_number,
  cash_balance
) VALUES (
  'customer-222',
  '123456789',
  'customer-222@gmail.com',
  'Best Customer',
  'Sydney',
  '0400000000',
  99999999
);


--- Test trigger to update Cash_balance o customer after insert on deposit
INSERT INTO Deposit(transaction_id, trade_date, login_name, amount) VALUES (1234255, '2022-10-24', 'customer-222', 1899);
INSERT INTO Deposit(transaction_id, trade_date, login_name, amount) VALUES (1234256, '2022-10-25', 'customer-222', 1000);

---Test Trade trigger for calculating final_amount
INSERT INTO TRADE(
  transaction_id,
  trade_date,
  login_name,
  num_of_ETF,
  price_per_ETF,
  trade_type,
  etf_code
) VALUES (
  DEFAULT,
  '2022-09-23',
  'customer-222',
  2002,
  10000,
  'BUY',
  'ETF-1'
);

--Select * from Holding;

-- Test Sell 
INSERT INTO TRADE(
  transaction_id,
  trade_date,
  login_name,
  num_of_ETF,
  price_per_ETF,
  trade_type,
  etf_code
) VALUES (
  DEFAULT,
  '2022-09-23',
  'customer-222',
  2009,
  10000,
  'SELL',
  'ETF-1'
);

-- Test Sell 
INSERT INTO Regular_Invest(
  transaction_id,
  login_name,
  num_of_ETF,
  price_per_ETF,
  etf_code,
  start_date,
  frequency
) VALUES (
  DEFAULT,
  'customer-222',
  2009,
  10000,
  'ETF-1',
  '2022-09-23',
  'MONTHLY'
);

INSERT INTO TRADE(
  transaction_id,
  trade_date,
  login_name,
  num_of_ETF,
  price_per_ETF,
  trade_type,
  etf_code
) VALUES (
  DEFAULT,
  '2022-09-23',
  'customer-111',
  100,
  100,
  'BUY',
  'ETF-1'
);

INSERT INTO CLOSING_PRICE_HISTORY(closing_price, date, units_outstanding, etf_code)
VALUES (299, '2022-09-23', 1000,'ETF-1');

Select * FROM Person;
Select * FROM Administrator;
Select * FROM Qualification;
Select * FROM Customer;
Select * FROM Trade;
Select * FROM Deposit;
Select * FROM Holding;
Select * FROM Regular_Invest;
Select * FROM ETF;
Select * FROM CLOSING_PRICE_HISTORY;





