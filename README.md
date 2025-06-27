zold_DiscountTable
ColumnName	Data Type	Allow Nulls
Id	int	Unchecked
TableName	varchar(64)	Unchecked
CountryId	int	Unchecked
Cohort	varchar(8)	Unchecked
CurrencyId	int	Unchecked
IsActive	bit	Unchecked
VaryIndicator	bit	Checked
RiPortfolio	varchar(255)	Checked
Slot	varchar(255)	Checked
RateType	varchar(8)	Checked
Ifrs17Portfolio	varchar(255)	Checked
Segment	varchar(32)	Checked
Product	varchar(32)	Checked

zold_DiscountTableVersion
ColumnName	Data Type	Allow Nulls
Id	int	Unchecked
DiscountTableId	int	Unchecked
VersionNo	int	Unchecked
InterpolationMethod	varchar(255)	Checked
ReferencePortfolio	varchar(255)	Unchecked
Mode	varchar(255)	Checked
Sensitivity	varchar(64)	Checked
Reset	varchar(64)	Checked
ObservablePeriod	int	Checked
InterpolationPeriod	int	Checked
CurveAdjustment	varchar(255)	Checked
UltimateRate	decimal(28, 20)	Checked
YieldCurveBasis	varchar(64)	Checked
IsActive	bit	Unchecked
CreatedByUserId	varchar(64)	Unchecked
CreatedTimestamp	datetime	Unchecked
AxisFormatting	int	Checked
Warning	varchar(40)	Checked
ReducedFlag	tinyint	Checked
ReducedFlagALDA	tinyint	Checked
EsgColumn	varchar(4)	Checked

zold_DiscountTableVersionRate
ColumnName	Data Type	Allow Nulls
Id	bigint	Unchecked
DiscountTableVersionId	int	Unchecked
Row	decimal(28, 20)	Unchecked
Value	decimal(28, 20)	Unchecked

DiscountRate and DiscountRateInactive
Name	varchar(64)	Unchecked
VersionNo	int	Unchecked
Rates	nvarchar(MAX)	Unchecked
CountryId	int	Unchecked
AxisCyHeader	decimal(18, 2)	Checked
Cohort	varchar(8)	Unchecked
CurrencyId	int	Unchecked
VaryIndicator	bit	Checked
RiPortfolio	varchar(255)	Checked
Slot	varchar(255)	Checked
RateType	varchar(8)	Unchecked
Ifrs17Portfolio	varchar(255)	Checked
Segment	varchar(32)	Checked
Product	varchar(32)	Checked
InterpolationMethod	varchar(255)	Checked
ReferencePortfolio	varchar(255)	Unchecked
Mode	varchar(255)	Checked
Sensitivity	varchar(64)	Checked
Reset	varchar(64)	Checked
ObservablePeriod	int	Checked
InterpolationPeriod	int	Checked
CurveAdjustment	varchar(255)	Checked
UltimateRate	decimal(28, 20)	Checked
YieldCurveBasis	varchar(64)	Checked
AxisFormatting	int	Checked
Warning	varchar(40)	Checked
ReducedFlag	int	Checked
ReducedFlagALDA	int	Checked
EsgColumn	varchar(4)	Checked
CreatedByUserId	varchar(64)	Unchecked
CreatedTimestamp	datetime	Unchecked
ValuationQuarter	int	Checked
ValuationYear	int	Checked


