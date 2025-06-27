SET NOCOUNT ON;

-- Parameters
DECLARE @BatchSize INT = 500000;
DECLARE @RowsAffected INT = 1;

WHILE @RowsAffected > 0
BEGIN
    BEGIN TRANSACTION;

    -- Staging duplicates-free versions for current batch
    WITH VersionChecksums AS (
        SELECT TOP (@BatchSize)
            v.Id AS DiscountTableVersionId,
            v.DiscountTableId,
            v.CreatedTimestamp,
            v.CreatedByUserId,
            HASHBYTES('MD5', STRING_AGG(CAST(vr.Row AS VARCHAR(40)) + ':' + CAST(vr.Value AS VARCHAR(40)), '|') WITHIN GROUP (ORDER BY vr.Row)) AS RatesChecksum
        FROM zold_DiscountTableVersion v
        JOIN zold_DiscountTableVersionRate vr ON v.Id = vr.DiscountTableVersionId
        WHERE v.IsActive = 0
          AND NOT EXISTS (
              SELECT 1 FROM DiscountRateInactive dri
              WHERE dri.Name = (SELECT TableName FROM zold_DiscountTable dt WHERE dt.Id = v.DiscountTableId)
                AND dri.CreatedTimestamp = v.CreatedTimestamp
          )
        GROUP BY v.Id, v.DiscountTableId, v.CreatedTimestamp, v.CreatedByUserId
    ),
    DedupedVersions AS (
        SELECT vc.*, 
               LAG(RatesChecksum) OVER (PARTITION BY DiscountTableId ORDER BY CreatedTimestamp) AS PrevChecksum
        FROM VersionChecksums vc
    ),
    CleanedVersions AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY DiscountTableId ORDER BY CreatedTimestamp) AS NewVersionNo
        FROM DedupedVersions
        WHERE RatesChecksum != ISNULL(PrevChecksum, 0x00)
    )

    -- Insert compressed data into DiscountRateInactive
    INSERT INTO DiscountRateInactive (
        Name, VersionNo, Rates, CountryId, Cohort, CurrencyId, RateType,
        CreatedTimestamp, CreatedByUserId, ValuationQuarter, ValuationYear
    )
    SELECT
        dt.TableName,
        cv.NewVersionNo,
        -- JSON Compression inline via RLE
        (SELECT 
            CASE WHEN dt.RateType = 'BEY' THEN
                JSON_QUERY('[' + STRING_AGG(
                    '{"value":' + CAST(grouped.Value AS VARCHAR(30)) +
                    ',"tenor":' + CAST(grouped.Row AS VARCHAR(30)) + '}', ','
                    ) WITHIN GROUP (ORDER BY grouped.Row) + ']')
            ELSE
                JSON_QUERY('[' + STRING_AGG(
                    '{"value":' + CAST(grouped.Value AS VARCHAR(30)) +
                    ',"length":' + CAST(COUNT(*) AS VARCHAR(10)) + '}', ','
                ) WITHIN GROUP (ORDER BY MIN(grouped.Row)) + ']')
            END
         FROM (
            SELECT vr.Row, vr.Value,
                ROW_NUMBER() OVER (ORDER BY vr.Row) - 
                ROW_NUMBER() OVER (PARTITION BY vr.Value ORDER BY vr.Row) AS grp
            FROM zold_DiscountTableVersionRate vr
            WHERE vr.DiscountTableVersionId = cv.DiscountTableVersionId
        ) grouped
        GROUP BY grouped.Value, grp
        ) AS Rates,
        dt.CountryId,
        dt.Cohort,
        dt.CurrencyId,
        ISNULL(dt.RateType, 'Standard'),
        cv.CreatedTimestamp,
        cv.CreatedByUserId,
        CASE 
            WHEN MONTH(cv.CreatedTimestamp) BETWEEN 1 AND 3 THEN 4
            WHEN MONTH(cv.CreatedTimestamp) BETWEEN 4 AND 6 THEN 1
            WHEN MONTH(cv.CreatedTimestamp) BETWEEN 7 AND 9 THEN 2
            ELSE 3 END AS ValuationQuarter,
        YEAR(DATEADD(QUARTER, -1, cv.CreatedTimestamp)) AS ValuationYear
    FROM CleanedVersions cv
    JOIN zold_DiscountTable dt ON dt.Id = cv.DiscountTableId;

    SET @RowsAffected = @@ROWCOUNT;
    COMMIT TRANSACTION;

    -- Progress indicator
    PRINT CONCAT(CAST(GETDATE() AS VARCHAR), ' - Migrated Batch: ', @RowsAffected, ' rows');
END

-- Update Active Versions if needed
WITH LatestVersion AS (
    SELECT Name, MAX(VersionNo) AS MaxVersionNo
    FROM DiscountRateInactive
    GROUP BY Name
)
UPDATE dr
SET dr.VersionNo = lv.MaxVersionNo
FROM DiscountRate dr
JOIN LatestVersion lv ON dr.Name = lv.Name
WHERE dr.VersionNo != lv.MaxVersionNo;

PRINT 'Migration Completed.';
