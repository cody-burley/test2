/*=====================================================================================
  ONE‑SHOT, UNDER‑10‑DAY MIGRATION SCRIPT FOR SQL SERVER HS_Gen5_32
  – Deduplicates old inactive versions, RLE‑ or tenor‑encodes, assigns quarter/year,
    and bulk‑inserts into DiscountRateInactive in a single execution.
  – Uses a staging table with a clustered columnstore index for speed.
  – Minimal logging via TABLOCK; parallelized with MAXDOP.
  – Drop the staging objects and helper index at the end.
=====================================================================================*/

---------------------------------------------------------------------------------------
-- OPTIONAL: switch to BULK_LOGGED for minimal logging (requires ALTER DATABASE perms)
---------------------------------------------------------------------------------------
-- ALTER DATABASE CURRENT SET RECOVERY BULK_LOGGED;
-- GO

---------------------------------------------------------------------------------------
-- 1) Create a temporary helper index on the old RateValue table to speed JSON/RLE scans
---------------------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
     WHERE object_id = OBJECT_ID('dbo.zold_DiscountTableVersionRate')
       AND name = 'IX_TMP_VVR_VersionId_Value_Row'
)
BEGIN
    PRINT '>> Creating helper index on zold_DiscountTableVersionRate';
    CREATE NONCLUSTERED INDEX IX_TMP_VVR_VersionId_Value_Row
      ON dbo.zold_DiscountTableVersionRate(DiscountTableVersionId, Value, [Row])
      WITH (ONLINE = ON);
END
GO

---------------------------------------------------------------------------------------
-- 2) Drop & re-create a staging table with the same schema as DiscountRateInactive
---------------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Stg_DiscountRateInactive','U') IS NOT NULL
    DROP TABLE dbo.Stg_DiscountRateInactive;
GO

CREATE TABLE dbo.Stg_DiscountRateInactive (
    [Name]                  varchar(64)    NOT NULL,
    VersionNo               int            NOT NULL,
    Rates                   nvarchar(MAX)  NOT NULL,
    CountryId               int            NOT NULL,
    AxisCyHeader            decimal(18,2)  NULL,
    Cohort                  varchar(8)     NOT NULL,
    CurrencyId              int            NOT NULL,
    VaryIndicator           bit            NULL,
    RiPortfolio             varchar(255)   NULL,
    Slot                    varchar(255)   NULL,
    RateType                varchar(8)     NOT NULL,
    Ifrs17Portfolio         varchar(255)   NULL,
    Segment                 varchar(32)    NULL,
    Product                 varchar(32)    NULL,
    InterpolationMethod     varchar(255)   NULL,
    ReferencePortfolio      varchar(255)   NOT NULL,
    [Mode]                  varchar(255)   NULL,
    Sensitivity             varchar(64)    NULL,
    [Reset]                 varchar(64)    NULL,
    ObservablePeriod        int            NULL,
    InterpolationPeriod     int            NULL,
    CurveAdjustment         varchar(255)   NULL,
    UltimateRate            decimal(28,20) NULL,
    YieldCurveBasis         varchar(64)    NULL,
    AxisFormatting          int            NULL,
    [Warning]               varchar(40)    NULL,
    ReducedFlag             int            NULL,
    ReducedFlagALDA         int            NULL,
    EsgColumn               varchar(4)     NULL,
    CreatedByUserId         varchar(64)    NOT NULL,
    CreatedTimestamp        datetime       NOT NULL,
    ValuationQuarter        int            NOT NULL,
    ValuationYear           int            NOT NULL
);
GO

---------------------------------------------------------------------------------------
-- 3) Create a clustered columnstore index on staging for fastest downstream scans
---------------------------------------------------------------------------------------
CREATE CLUSTERED COLUMNSTORE INDEX CCI_Stg_DRInactive
  ON dbo.Stg_DiscountRateInactive;
GO

---------------------------------------------------------------------------------------
-- 4) Populate staging in one CTE‑driven pass
---------------------------------------------------------------------------------------
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
WITH
RawJson AS (
  SELECT
    v.Id               AS VersionId,
    t.TableName        AS [Name],
    v.DiscountTableId,
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
    /* Build JSON per-version: BEY uses tenor, others RLE-length */
    CASE WHEN t.RateType = 'BEY' THEN
      ( SELECT rv.Value AS [value], rv.[Row] AS [tenor]
        FROM dbo.zold_DiscountTableVersionRate AS rv
        WHERE rv.DiscountTableVersionId = v.Id
        ORDER BY rv.[Row]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
      )
    ELSE
      ( SELECT run.Value AS [value], COUNT(*) AS [length]
        FROM (
          SELECT
            rv.Value,
            rv.[Row],
            ROW_NUMBER() OVER (ORDER BY rv.[Row])
            - ROW_NUMBER() OVER (PARTITION BY rv.Value ORDER BY rv.[Row]) AS grp
          FROM dbo.zold_DiscountTableVersionRate AS rv
          WHERE rv.DiscountTableVersionId = v.Id
        ) AS run
        GROUP BY run.grp, run.Value
        ORDER BY MIN(run.[Row])
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
      )
    END AS RatesJson
  FROM dbo.zold_DiscountTableVersion AS v
  JOIN dbo.zold_DiscountTable        AS t
    ON t.Id = v.DiscountTableId
  WHERE v.IsActive = 0
),
Ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER(
      PARTITION BY r.DiscountTableId
      ORDER BY r.CreatedTimestamp, r.VersionId
    )                    AS Ordinal,
    LAG(r.RatesJson) OVER(
      PARTITION BY r.DiscountTableId
      ORDER BY r.CreatedTimestamp, r.VersionId
    )                    AS PrevJson
  FROM RawJson AS r
),
UniqueVersions AS (
  SELECT
    ru.VersionId,
    ru.[Name],
    ru.RatesJson,
    ru.CountryId,
    ru.Cohort,
    ru.CurrencyId,
    ru.VaryIndicator,
    ru.RiPortfolio,
    ru.Slot,
    ru.RateType,
    ru.Ifrs17Portfolio,
    ru.Segment,
    ru.Product,
    ru.InterpolationMethod,
    ru.ReferencePortfolio,
    ru.[Mode],
    ru.Sensitivity,
    ru.[Reset],
    ru.ObservablePeriod,
    ru.InterpolationPeriod,
    ru.CurveAdjustment,
    ru.UltimateRate,
    ru.YieldCurveBasis,
    ru.AxisFormatting,
    ru.[Warning],
    ru.ReducedFlag,
    ru.ReducedFlagALDA,
    ru.EsgColumn,
    ru.CreatedByUserId,
    ru.CreatedTimestamp,
    /* determine quarter & year one behind */
    CASE WHEN DATEPART(QUARTER, ru.CreatedTimestamp)=1 THEN 4 ELSE DATEPART(QUARTER, ru.CreatedTimestamp)-1 END AS ValuationQuarter,
    CASE WHEN DATEPART(QUARTER, ru.CreatedTimestamp)=1 THEN YEAR(ru.CreatedTimestamp)-1 ELSE YEAR(ru.CreatedTimestamp) END AS ValuationYear,
    ROW_NUMBER() OVER(
      PARTITION BY ru.DiscountTableId
      ORDER BY ru.CreatedTimestamp, ru.VersionId
    )                    AS NewVersionNo
  FROM Ranked AS ru
  WHERE ru.PrevJson IS NULL
     OR ru.RatesJson <> ru.PrevJson
)
SELECT
  u.[Name],
  u.NewVersionNo     AS VersionNo,
  u.RatesJson        AS Rates,
  u.CountryId,
  CAST(NULL AS DECIMAL(18,2)) AS AxisCyHeader,
  u.Cohort,
  u.CurrencyId,
  u.VaryIndicator,
  u.RiPortfolio,
  u.Slot,
  u.RateType,
  u.Ifrs17Portfolio,
  u.Segment,
  u.Product,
  u.InterpolationMethod,
  u.ReferencePortfolio,
  u.[Mode],
  u.Sensitivity,
  u.[Reset],
  u.ObservablePeriod,
  u.InterpolationPeriod,
  u.CurveAdjustment,
  u.UltimateRate,
  u.YieldCurveBasis,
  u.AxisFormatting,
  u.[Warning],
  u.ReducedFlag,
  u.ReducedFlagALDA,
  u.EsgColumn,
  u.CreatedByUserId,
  u.CreatedTimestamp,
  u.ValuationQuarter,
  u.ValuationYear
FROM UniqueVersions AS u
OPTION (MAXDOP 0);
GO

---------------------------------------------------------------------------------------
-- 5) Update active DiscountRate.VersionNo to follow last inactive
---------------------------------------------------------------------------------------
UPDATE dr
SET dr.VersionNo = seq.MaxInactive + 1
FROM dbo.DiscountRate AS dr
JOIN (
  SELECT [Name], MAX(NewVersionNo) AS MaxInactive
  FROM dbo.Stg_DiscountRateInactive
  GROUP BY [Name]
) AS seq
  ON dr.[Name] = seq.[Name];
GO

---------------------------------------------------------------------------------------
-- 6) Merge staging into live DiscountRateInactive
---------------------------------------------------------------------------------------
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

---------------------------------------------------------------------------------------
-- 7) Cleanup: drop staging table and helper index, and revert recovery if needed
---------------------------------------------------------------------------------------
DROP TABLE dbo.Stg_DiscountRateInactive;
GO

DROP INDEX IX_TMP_VVR_VersionId_Value_Row
  ON dbo.zold_DiscountTableVersionRate;
GO

-- OPTIONAL: revert to FULL recovery
-- ALTER DATABASE CURRENT SET RECOVERY FULL;
-- GO
