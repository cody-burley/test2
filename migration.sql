SET NOCOUNT ON;

-- ─────────────────────────────────────────
-- Phase 0: Batch‑insert missing, deduped versions
-- ─────────────────────────────────────────
DECLARE @BatchSize   INT = 250000,
        @RowsAffected INT = 1;

WHILE @RowsAffected > 0
BEGIN
    BEGIN TRANSACTION;

    WITH VersionChecksums AS (
        SELECT TOP (@BatchSize)
            v.Id                   AS DiscountTableVersionId,
            v.DiscountTableId,
            dt.TableName,
            v.VersionNo,
            v.CreatedTimestamp,
            v.CreatedByUserId,
            HASHBYTES(
              'MD5',
              STRING_AGG(
                CAST(vr.Row   AS VARCHAR(40)) + ':' +
                CAST(vr.Value AS VARCHAR(40)),
                '|' 
              ) WITHIN GROUP (ORDER BY vr.Row)
            ) AS RatesChecksum
        FROM zold_DiscountTableVersion v
        INNER JOIN zold_DiscountTable dt
           ON v.DiscountTableId = dt.Id
        INNER JOIN zold_DiscountTableVersionRate vr
           ON vr.DiscountTableVersionId = v.Id
        LEFT JOIN DiscountRateInactive dri
           ON dri.Name      = dt.TableName
          AND dri.VersionNo = v.VersionNo
        WHERE v.IsActive = 0
          AND dri.Name IS NULL    -- skip any Name+VersionNo already present
        GROUP BY
          v.Id, v.DiscountTableId, dt.TableName,
          v.VersionNo, v.CreatedTimestamp, v.CreatedByUserId
    ),
    DedupedVersions AS (
        SELECT
          vc.*,
          LAG(vc.RatesChecksum) 
            OVER (PARTITION BY vc.DiscountTableId 
                  ORDER BY vc.CreatedTimestamp) AS PrevChecksum
        FROM VersionChecksums vc
    ),
    CleanedVersions AS (
        SELECT
          *,
          ROW_NUMBER() 
            OVER (PARTITION BY DiscountTableId 
                  ORDER BY CreatedTimestamp) AS NewVersionSequence
        FROM DedupedVersions
        WHERE RatesChecksum != ISNULL(PrevChecksum, 0x00)
    )

    INSERT INTO DiscountRateInactive (
      Name, VersionNo, Rates, CountryId, Cohort, CurrencyId, RateType,
      CreatedTimestamp, CreatedByUserId, ValuationQuarter, ValuationYear
    )
    SELECT
      cv.TableName,
      cv.VersionNo,
      (
        SELECT 
          CASE
            WHEN dt.RateType = 'BEY' THEN
              JSON_QUERY(
                '[' +
                STRING_AGG(
                  '{"value":' + CAST(g.Value AS VARCHAR(30)) +
                  ',"tenor":' + CAST(g.Row   AS VARCHAR(30)) + '}',
                  ','
                ) WITHIN GROUP (ORDER BY g.Row)
                + ']'
              )
            ELSE
              JSON_QUERY(
                '[' +
                STRING_AGG(
                  '{"value":'  + CAST(g.Value AS VARCHAR(30)) +
                  ',"length":' + CAST(COUNT(*) AS VARCHAR(10)) + '}',
                  ','
                ) WITHIN GROUP (ORDER BY MIN(g.Row))
                + ']'
              )
          END
        FROM (
          SELECT
            vr.Row,
            vr.Value,
            ROW_NUMBER() OVER (ORDER BY vr.Row)
            - ROW_NUMBER() OVER (PARTITION BY vr.Value ORDER BY vr.Row) AS grp
          FROM zold_DiscountTableVersionRate vr
          WHERE vr.DiscountTableVersionId = cv.DiscountTableVersionId
        ) AS g
        GROUP BY g.Value, g.grp
      ) AS Rates,
      dt.CountryId,
      dt.Cohort,
      dt.CurrencyId,
      dt.RateType,  -- <— no default applied; may be NULL
      cv.CreatedTimestamp,
      cv.CreatedByUserId,
      CASE 
        WHEN MONTH(cv.CreatedTimestamp) BETWEEN 1 AND 3 THEN 4
        WHEN MONTH(cv.CreatedTimestamp) BETWEEN 4 AND 6 THEN 1
        WHEN MONTH(cv.CreatedTimestamp) BETWEEN 7 AND 9 THEN 2
        ELSE 3
      END AS ValuationQuarter,
      YEAR(DATEADD(QUARTER, -1, cv.CreatedTimestamp)) AS ValuationYear
    FROM CleanedVersions AS cv
    JOIN zold_DiscountTable AS dt
      ON dt.Id = cv.DiscountTableId;

    SET @RowsAffected = @@ROWCOUNT;
    COMMIT TRANSACTION;

    PRINT CONCAT(CAST(GETDATE() AS VARCHAR), ' → Migrated Batch: ', @RowsAffected, ' rows');
END


-- ─────────────────────────────────────────
-- Phase 1: Re‑sequence every history to 1…N 
-- ─────────────────────────────────────────
;WITH Renumber AS (
  SELECT
    Name,
    CreatedTimestamp,
    ROW_NUMBER() OVER (PARTITION BY Name ORDER BY CreatedTimestamp) AS CorrectVersionNo
  FROM DiscountRateInactive
)
UPDATE dri
SET VersionNo = r.CorrectVersionNo
FROM DiscountRateInactive AS dri
JOIN Renumber              AS r
  ON dri.Name             = r.Name
 AND dri.CreatedTimestamp = r.CreatedTimestamp;


-- ─────────────────────────────────────────
-- Phase 2: Sync “active” table to one higher than history
-- ─────────────────────────────────────────
;WITH LatestInactive AS (
  SELECT Name, MAX(VersionNo) AS MaxInactiveVersion
  FROM DiscountRateInactive
  GROUP BY Name
)
UPDATE dr
SET dr.VersionNo = li.MaxInactiveVersion + 1
FROM DiscountRate      AS dr
JOIN LatestInactive   AS li
  ON dr.Name = li.Name
WHERE dr.VersionNo != li.MaxInactiveVersion + 1;


PRINT '⚡️ Full migration and resequence complete!';
