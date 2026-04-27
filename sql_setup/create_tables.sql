-- ============================================================================
-- Capital Markets ETL - Azure SQL Database Setup
-- Run this in Azure SQL DB to create source tables
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'source')
    EXEC('CREATE SCHEMA source');
GO

-- INSTRUMENTS
IF OBJECT_ID('source.instruments', 'U') IS NOT NULL DROP TABLE source.instruments;
GO
CREATE TABLE source.instruments (
    ticker          VARCHAR(10)     PRIMARY KEY,
    company_name    VARCHAR(100)    NOT NULL,
    sector          VARCHAR(50)     NOT NULL,
    asset_class     VARCHAR(20)     NOT NULL,
    currency        VARCHAR(3)      NOT NULL DEFAULT 'USD',
    exchange        VARCHAR(10)     NOT NULL,
    base_price      DECIMAL(12,2)   NOT NULL,
    is_active       BIT             NOT NULL DEFAULT 1,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);
GO

-- TRADERS
IF OBJECT_ID('source.traders', 'U') IS NOT NULL DROP TABLE source.traders;
GO
CREATE TABLE source.traders (
    trader_id       VARCHAR(10)     PRIMARY KEY,
    trader_name     VARCHAR(100)    NOT NULL,
    desk            VARCHAR(10)     NOT NULL,
    desk_full_name  VARCHAR(50)     NOT NULL,
    risk_limit_usd  DECIMAL(15,2)   NOT NULL,
    manager         VARCHAR(100)    NULL,
    location        VARCHAR(50)     NOT NULL,
    is_active       BIT             NOT NULL DEFAULT 1,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);
GO

-- COUNTERPARTIES
IF OBJECT_ID('source.counterparties', 'U') IS NOT NULL DROP TABLE source.counterparties;
GO
CREATE TABLE source.counterparties (
    counterparty_id     VARCHAR(10)     PRIMARY KEY,
    counterparty_name   VARCHAR(100)    NOT NULL,
    lei_code            VARCHAR(20)     NOT NULL,
    risk_rating         VARCHAR(10)     NOT NULL,
    counterparty_type   VARCHAR(20)     NOT NULL,
    country             VARCHAR(5)      NOT NULL,
    city                VARCHAR(50)     NOT NULL,
    is_active           BIT             NOT NULL DEFAULT 1,
    created_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);
GO

-- HISTORICAL TRADES (for backfill)
IF OBJECT_ID('source.historical_trades', 'U') IS NOT NULL DROP TABLE source.historical_trades;
GO
CREATE TABLE source.historical_trades (
    trade_id        VARCHAR(30)     PRIMARY KEY,
    ticker          VARCHAR(10)     NOT NULL,
    side            VARCHAR(4)      NOT NULL,
    quantity        INT             NOT NULL,
    price           DECIMAL(12,2)   NOT NULL,
    order_type      VARCHAR(15)     NOT NULL,
    trader_id       VARCHAR(10)     NOT NULL,
    desk            VARCHAR(10)     NOT NULL,
    counterparty_id VARCHAR(10)     NOT NULL,
    exchange        VARCHAR(10)     NOT NULL,
    currency        VARCHAR(3)      NOT NULL DEFAULT 'USD',
    asset_class     VARCHAR(20)     NOT NULL,
    trade_status    VARCHAR(15)     NOT NULL DEFAULT 'SETTLED',
    trade_date      DATE            NOT NULL,
    settlement_date DATE            NULL,
    timestamp       DATETIME2       NOT NULL,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX IX_hist_ticker ON source.historical_trades(ticker);
CREATE INDEX IX_hist_trader ON source.historical_trades(trader_id);
CREATE INDEX IX_hist_date   ON source.historical_trades(trade_date);
CREATE INDEX IX_hist_desk   ON source.historical_trades(desk);
GO

PRINT 'All source tables created successfully.';
GO
