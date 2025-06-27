--------------------------------------------------------------------------------
-- 1) Helper index on the old RateValue table (dropped at the end)
--------------------------------------------------------------------------------
IF NOT EXISTS(
    SELECT 1 FROM sys.indexes 
     WHERE object_id = OBJECT_ID('dbo.zold_DiscountTableVersionRate')
       AND name = 'IX_TMP_VVR_VersionId_Value_Row'
)
BEGIN
    PRINT '>> Creating helper index on RateValue';
    CREATE NONCLUSTERED INDEX IX_TMP_VVR_VersionId_Value_Row
      ON dbo.zold_DiscountTableVersionRate(
         DiscountTableVersionId, Value, [Row]
      ) WITH (ONLINE=ON);
END
GO

--------------------------------------------------------------------------------
-- 2) Drop & recreate staging with a non‑clustered PK and clustered columnstore
--------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Stg_DiscountRateInactive','U') IS NOT NULL
  DROP TABLE dbo.Stg_DiscountRateInactive;
GO

-- 1) Create the staging table — no inline PK or indexes here
CREATE TABLE dbo.Stg_DiscountRateInactive (
    StgID             BIGINT IDENTITY(1,1) NOT NULL,
    [Name]            VARCHAR(64)    NOT NULL,
    VersionNo         INT            NOT NULL,
    Rates             NVARCHAR(MAX)  NOT NULL,
    CountryId         INT            NOT NULL,
    AxisCyHeader      DECIMAL(18,2)  NULL,
    Cohort            VARCHAR(8)     NOT NULL,
    CurrencyId        INT            NOT NULL,
    VaryIndicator     BIT            NULL,
    RiPortfolio       VARCHAR(255)   NULL,
    Slot              VARCHAR(255)   NULL,
    RateType          VARCHAR(8)     NOT NULL,
    Ifrs17Portfolio   VARCHAR(255)   NULL,
    Segment           VARCHAR(32)    NULL,
    Product           VARCHAR(32)    NULL,
    InterpolationMethod VARCHAR(255) NULL,
    ReferencePortfolio VARCHAR(255)  NOT NULL,
    [Mode]            VARCHAR(255)   NULL,
    Sensitivity       VARCHAR(64)    NULL,
    [Reset]           VARCHAR(64)    NULL,
    ObservablePeriod  INT            NULL,
    InterpolationPeriod INT          NULL,
    CurveAdjustment   VARCHAR(255)   NULL,
    UltimateRate      DECIMAL(28,20) NULL,
    YieldCurveBasis   VARCHAR(64)    NULL,
    AxisFormatting    INT            NULL,
    [Warning]         VARCHAR(40)    NULL,
    ReducedFlag       INT            NULL,
    ReducedFlagALDA   INT            NULL,
    EsgColumn         VARCHAR(4)     NULL,
    CreatedByUserId   VARCHAR(64)    NOT NULL,
    CreatedTimestamp  DATETIME       NOT NULL,
    ValuationQuarter  INT            NOT NULL,
    ValuationYear     INT            NOT NULL
);
GO

IF OBJECT_ID('dbo.Stg_DiscountRateInactive','U') IS NOT NULL
BEGIN
  DECLARE @ci NVARCHAR(128);
  SELECT @ci = name 
  FROM sys.indexes 
  WHERE object_id = OBJECT_ID('dbo.Stg_DiscountRateInactive')
    AND type_desc = 'CLUSTERED';

  IF @ci IS NOT NULL
  BEGIN
    PRINT 'Dropping existing clustered index: ' + @ci;
    EXEC(N'DROP INDEX ' + QUOTENAME(@ci) + ' ON dbo.Stg_DiscountRateInactive;');
  END
END
GO
    
-- 2) Turn it into a clustered columnstore (no columns or INCLUDE allowed)
CREATE CLUSTERED COLUMNSTORE INDEX CCI_Stg_DRInactive
  ON dbo.Stg_DiscountRateInactive;
GO

-- 3) Now add a nonclustered primary key on StgID
ALTER TABLE dbo.Stg_DiscountRateInactive
  ADD CONSTRAINT PK_Stg_DRInact
      PRIMARY KEY NONCLUSTERED (StgID);
GO


--------------------------------------------------------------------------------
-- 3) Determine the VersionId range & batch size
--------------------------------------------------------------------------------
DECLARE
  @MinId     INT, 
  @MaxId     INT, 
  @BatchSize INT = 2000000,       -- ~2 million versions per chunk; tune as needed
  @StartId   INT,
  @EndId     INT;

SELECT 
  @MinId = MIN(v.Id),
  @MaxId = MAX(v.Id)
FROM dbo.zold_DiscountTableVersion AS v
WHERE v.IsActive = 0;

SET @StartId = @MinId;

--------------------------------------------------------------------------------
-- 4) Loop: build JSON + dedupe + quarter/year + insert into staging
--------------------------------------------------------------------------------
WHILE @StartId <= @MaxId
BEGIN
  SET @EndId = @StartId + @BatchSize - 1;
  PRINT '>> Processing VersionId ' 
        + CAST(@StartId AS VARCHAR(10)) 
        + ' to ' 
        + CAST(@EndId AS VARCHAR(10));

  BEGIN TRAN;

    ;WITH RawJson AS (
      SELECT
        v.Id                    AS VersionId,
        t.TableName             AS [Name],
        t.CountryId,
        t.Cohort,
        t.CurrencyId,
        t.VaryIndicator,
        t.RiPortfolio,
        t.Slot,
        t.RateType,
        t.Ifrs17Portfolio,
        t.Segment,
        t.Product,
        v.InterpolationMethod,
        v.ReferencePortfolio,
        v.[Mode],
        v.Sensitivity,
        v.[Reset],
        v.ObservablePeriod,
        v.InterpolationPeriod,
        v.CurveAdjustment,
        v.UltimateRate,
        v.YieldCurveBasis,
        v.AxisFormatting,
        v.[Warning],
        v.ReducedFlag,
        v.ReducedFlagALDA,
        v.EsgColumn,
        v.CreatedByUserId,
        v.CreatedTimestamp,
        /* JSON branch: BEY uses tenor, others use RLE length */
        CASE 
          WHEN t.RateType='BEY' THEN
            ( SELECT rv.Value AS [value], rv.[Row] AS [tenor]
              FROM dbo.zold_DiscountTableVersionRate rv
              WHERE rv.DiscountTableVersionId=v.Id
              ORDER BY rv.[Row]
              FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
          ELSE
            ( SELECT run.Value  AS [value],
                     COUNT(*)   AS [length]
              FROM (
                SELECT
                  rv.Value,
                  rv.[Row],
                  ROW_NUMBER() OVER(ORDER BY rv.[Row])
                  - ROW_NUMBER() OVER(PARTITION BY rv.Value ORDER BY rv.[Row]) AS grp
                FROM dbo.zold_DiscountTableVersionRate rv
                WHERE rv.DiscountTableVersionId=v.Id
              ) AS run
              GROUP BY run.grp, run.Value
              ORDER BY MIN(run.[Row])
              FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        END AS RatesJson
      FROM dbo.zold_DiscountTableVersion v
      JOIN dbo.zold_DiscountTable        t 
        ON t.Id = v.DiscountTableId
      WHERE v.IsActive = 0
        AND v.Id BETWEEN @StartId AND @EndId
    ),
    Ranked AS (
      SELECT
        r.*,
        ROW_NUMBER() OVER(
          PARTITION BY r.[Name]
          ORDER BY r.CreatedTimestamp, r.VersionId
        )                  AS Ordinal,
        LAG(r.RatesJson) OVER(
          PARTITION BY r.[Name]
          ORDER BY r.CreatedTimestamp, r.VersionId
        )                  AS PrevJson
      FROM RawJson AS r
    ),
    UniqueVersions AS (
      SELECT
        ru.*
      FROM Ranked AS ru
      WHERE ru.PrevJson IS NULL
         OR ru.RatesJson <> ru.PrevJson
    )

    INSERT INTO dbo.Stg_DiscountRateInactive WITH (TABLOCK)
    (
      [Name], VersionNo, Rates, CountryId, AxisCyHeader,
      Cohort, CurrencyId, VaryIndicator, RiPortfolio, Slot,
      RateType, Ifrs17Portfolio, Segment, Product,
      InterpolationMethod, ReferencePortfolio, [Mode], Sensitivity, [Reset],
      ObservablePeriod, InterpolationPeriod, CurveAdjustment, UltimateRate,
      YieldCurveBasis, AxisFormatting, [Warning], ReducedFlag, ReducedFlagALDA,
      EsgColumn, CreatedByUserId, CreatedTimestamp,
      ValuationQuarter, ValuationYear
    )
    SELECT
      uv.[Name],
      0                           AS VersionNo,       -- placeholder
      uv.RatesJson               AS Rates,
      uv.CountryId,
      NULL                        AS AxisCyHeader,
      uv.Cohort,
      uv.CurrencyId,
      uv.VaryIndicator,
      uv.RiPortfolio,
      uv.Slot,
      uv.RateType,
      uv.Ifrs17Portfolio,
      uv.Segment,
      uv.Product,
      uv.InterpolationMethod,
      uv.ReferencePortfolio,
      uv.[Mode],
      uv.Sensitivity,
      uv.[Reset],
      uv.ObservablePeriod,
      uv.InterpolationPeriod,
      uv.CurveAdjustment,
      uv.UltimateRate,
      uv.YieldCurveBasis,
      uv.AxisFormatting,
      uv.[Warning],
      uv.ReducedFlag,
      uv.ReducedFlagALDA,
      uv.EsgColumn,
      uv.CreatedByUserId,
      uv.CreatedTimestamp,
      CASE WHEN DATEPART(QUARTER, uv.CreatedTimestamp)=1 THEN 4 
           ELSE DATEPART(QUARTER, uv.CreatedTimestamp)-1 
      END                        AS ValuationQuarter,
      CASE WHEN DATEPART(QUARTER, uv.CreatedTimestamp)=1 
           THEN YEAR(uv.CreatedTimestamp)-1 
           ELSE YEAR(uv.CreatedTimestamp) 
      END                        AS ValuationYear
    FROM UniqueVersions AS uv;

  COMMIT;

  SET @StartId = @EndId + 1;
END
GO

--------------------------------------------------------------------------------
-- 5) Once all batches are in staging, assign the real VersionNo via a window:
--------------------------------------------------------------------------------
;WITH Sequenced AS (
  SELECT
    StgID,
    ROW_NUMBER() OVER(
      PARTITION BY [Name]
      ORDER BY CreatedTimestamp, StgID
    ) AS NewVer
  FROM dbo.Stg_DiscountRateInactive
)
UPDATE st
SET st.VersionNo = sq.NewVer
FROM dbo.Stg_DiscountRateInactive st
JOIN Sequenced sq ON st.StgID = sq.StgID;
GO

--------------------------------------------------------------------------------
-- 6) Bump the live DiscountRate.VersionNo to follow the last inactive
--------------------------------------------------------------------------------
UPDATE dr
SET dr.VersionNo = seq.MaxInactive + 1
FROM dbo.DiscountRate dr
JOIN (
  SELECT [Name], MAX(VersionNo) AS MaxInactive
  FROM dbo.Stg_DiscountRateInactive
  GROUP BY [Name]
) AS seq
  ON dr.[Name] = seq.[Name];
GO

--------------------------------------------------------------------------------
-- 7) Bulk‑insert staging into the real table
--------------------------------------------------------------------------------
INSERT INTO dbo.DiscountRateInactive
(
  [Name], VersionNo, Rates, CountryId, AxisCyHeader,
  Cohort, CurrencyId, VaryIndicator, RiPortfolio, Slot,
  RateType, Ifrs17Portfolio, Segment, Product,
  InterpolationMethod, ReferencePortfolio, [Mode], Sensitivity, [Reset],
  ObservablePeriod, InterpolationPeriod, CurveAdjustment, UltimateRate,
  YieldCurveBasis, AxisFormatting, [Warning], ReducedFlag, ReducedFlagALDA,
  EsgColumn, CreatedByUserId, CreatedTimestamp,
  ValuationQuarter, ValuationYear
)
SELECT
  [Name], VersionNo, Rates, CountryId, AxisCyHeader,
  Cohort, CurrencyId, VaryIndicator, RiPortfolio, Slot,
  RateType, Ifrs17Portfolio, Segment, Product,
  InterpolationMethod, ReferencePortfolio, [Mode], Sensitivity, [Reset],
  ObservablePeriod, InterpolationPeriod, CurveAdjustment, UltimateRate,
  YieldCurveBasis, AxisFormatting, [Warning], ReducedFlag, ReducedFlagALDA,
  EsgColumn, CreatedByUserId, CreatedTimestamp,
  ValuationQuarter, ValuationYear
FROM dbo.Stg_DiscountRateInactive
WITH (TABLOCK)
OPTION (MAXDOP 0);
GO

--------------------------------------------------------------------------------
-- 8) Cleanup: drop staging & helper index
--------------------------------------------------------------------------------
DROP TABLE dbo.Stg_DiscountRateInactive;
GO

DROP INDEX IX_TMP_VVR_VersionId_Value_Row
  ON dbo.zold_DiscountTableVersionRate;
GO
