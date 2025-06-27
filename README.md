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

[{"value":0.00230056734773208650, "tenor":0.25000000000000000000},{"value":0.00228403626864234300, "tenor":0.50000000000000000000},{"value":0.00168566138354172800, "tenor":1.00000000000000000000},{"value":0.00240255395026045670, "tenor":2.00000000000000000000},{"value":0.00272178129393552560, "tenor":3.00000000000000000000},{"value":0.00334340417661146970, "tenor":5.00000000000000000000},{"value":0.00483214536049083400, "tenor":7.00000000000000000000},{"value":0.00667574718271178200, "tenor":10.00000000000000000000},{"value":0.00598738223251352500, "tenor":20.00000000000000000000},{"value":0.01126059920578585000, "tenor":30.00000000000000000000}]

[{"value":3.0401928165873757,"length":4},{"value":3.3316625402932036,"length":4},{"value":3.6225298089367897,"length":4},{"value":3.9235728254230344,"length":4},{"value":4.232017428171984,"length":4},{"value":5.012440214528515,"length":4},{"value":5.510366390136307,"length":4},{"value":5.46270185208631,"length":4},{"value":5.848779454176152,"length":4},{"value":6.255449803506857,"length":4},{"value":4.838157407810373,"length":4},{"value":4.905034237101128,"length":4},{"value":4.974279203144243,"length":4},{"value":5.04608366435138,"length":4},{"value":5.120658566212873,"length":4},{"value":5.198236983409659,"length":4},{"value":5.279077067323142,"length":4},{"value":5.363465476479977,"length":4},{"value":5.451721384875349,"length":4},{"value":5.544201185092246,"length":4},{"value":5.571819608411321,"length":4},{"value":5.665570473016322,"length":4},{"value":5.764296207793994,"length":4},{"value":5.868488471417029,"length":4},{"value":5.978703055723141,"length":4},{"value":6.095570635293592,"length":4},{"value":6.2198097536528785,"length":4},{"value":6.352242608643949,"length":4},{"value":6.493814367310896,"length":4},{"value":6.645616966791344,"length":4},{"value":3.655539452958449,"length":4},{"value":3.65553945295797,"length":4},{"value":3.655539452958649,"length":4},{"value":3.655539452957842,"length":4},{"value":3.6555394529586214,"length":4},{"value":3.6555394529578145,"length":4},{"value":3.655539452958688,"length":4},{"value":3.655539452957859,"length":4},{"value":3.6555394529582994,"length":4},{"value":3.6555394529585463,"length":4},{"value":3.65553945295788,"length":4},{"value":3.6555394529579393,"length":4},{"value":3.655539452958771,"length":4},{"value":3.6555394529584153,"length":4},{"value":3.655539452958329,"length":4},{"value":3.6555394529574055,"length":4},{"value":3.6555394529589536,"length":4},{"value":3.6555394529583207,"length":4},{"value":3.6555394529578638,"length":4},{"value":3.6555394529582945,"length":4},{"value":3.6555394529586884,"length":4},{"value":3.6555394529573277,"length":4},{"value":3.655539452958555,"length":4},{"value":3.6555394529578478,"length":4},{"value":3.655539452958856,"length":4},{"value":3.6555394529580405,"length":4},{"value":3.655539452958618,"length":4},{"value":3.655539452958538,"length":4},{"value":3.655539452957944,"length":4},{"value":3.6555394529582315,"length":4},{"value":3.655539452958257,"length":4},{"value":3.65553945295827,"length":4},{"value":3.655539452957479,"length":4},{"value":3.655539452959316,"length":4},{"value":3.6555394529572136,"length":4},{"value":3.6555394529593674,"length":4},{"value":3.655539452957061,"length":4},{"value":3.6555394529589473,"length":4},{"value":3.6555394529579144,"length":4},{"value":3.655539452958418,"length":4},{"value":4.35263639111684,"length":4},{"value":4.352636391116816,"length":52},{"value":4.352636391116793,"length":4},{"value":4.352636391116816,"length":12},{"value":4.352636391116793,"length":4},{"value":4.35263639111684,"length":4},{"value":4.352636391116816,"length":8},{"value":4.35263639111684,"length":4},{"value":4.352636391116816,"length":20},{"value":4.352636391116793,"length":4},{"value":4.352636391116816,"length":4}]

[{"value":2.53909128202862500000, "length":12},{"value":3.62546981350559030000, "length":4},{"value":4.68455625102737100000, "length":176},{"value":2.53909128202862500000, "length":208}]
