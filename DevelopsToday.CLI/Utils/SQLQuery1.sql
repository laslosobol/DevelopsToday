CREATE DATABASE DevelopsToday;
GO

USE DevelopsToday;
GO

CREATE TABLE ETLRecords
(
    id INT IDENTITY(1,1) PRIMARY KEY,
    TpepPickupDatetime DATETIME,
    TpepDropoffDatetime DATETIME,
    PassengerCount INT,
    TripDistance FLOAT,
    StoreAndFwdFlag VARCHAR(3) NOT NULL,
    PULocationID INT,
    DOLocationID INT,
    FareAmount FLOAT,
    TipAmount FLOAT
);
GO

CREATE PROCEDURE sp_GetTopTipLocationID 
AS
BEGIN
  SELECT TOP 1 PULocationID, AVG(TipAmount) AS avg_tip_amount
  FROM ETLRecords
  GROUP BY PULocationID
  ORDER BY avg_tip_amount DESC;
END;

CREATE PROCEDURE sp_GetTop100LongestFaresByDistance 
AS
BEGIN
  SELECT TOP 100 TpepPickupDatetime, TpepDropoffDatetime, TripDistance, FareAmount, TipAmount
  FROM ETLRecords
  ORDER BY TripDistance DESC;
END;

CREATE PROCEDURE GetTopLongestFaresTime
AS
BEGIN
    SELECT TOP 100 *
    FROM ETLRecords
    ORDER BY DATEDIFF(second, TpepPickupDatetime, TpepDropoffDatetime) DESC;
END;

CREATE PROCEDURE SearchByPULocationId
    @PULocationId INT
AS
BEGIN
    SELECT *
    FROM ETLRecords
    WHERE PULocationId = @PULocationId;
END;

CREATE INDEX idx_avg_tip_amount ON ETLRecords (PULocationId) INCLUDE (TipAmount);
CREATE INDEX idx_longest_fares_distance ON ETLRecords (TripDistance) INCLUDE (id);
CREATE INDEX idx_longest_fares_time_id ON ETLRecords (id) INCLUDE (TpepPickupDatetime, TpepDropoffDatetime)
CREATE INDEX idx_search_PULocationId ON ETLRecords (PULocationId);