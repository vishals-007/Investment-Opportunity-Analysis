from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Portfolio Analysis").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

from pyspark.sql.functions import *

stock_lst = ["AAPL","ABBV","ABT","ADBE","ADP","AMGN","AMT","AMZN","AROW","ARTNA","AXP","AZN","BA", \
             "BABA","BAC","BCAL","BHP","BKNG","BTI","CAT","CECO","CHEF","CL","CMRX","COCO","CRM", \
             "CRML","CSCO","CTOS", "CVX","DEO","DIS","ELLO","EQIX","ETN","FAT","GCMG","GE","GOOGL", \
             "GRC","GS","GWRS","HIMX","HIPO","HON","IBM","ICHR","ISRG","JNJ","JPM","KLG","KMB","KO", \
             "LIN","LLY","LMT","MA","MCD","MDLZ","META","MS","MSFT","MXL","NFLX","NKE","NSPR","NTES", \
             "NVDA","ORCL","PCYO","PDD","PFE","PG","PLCE","PM","PNNT","PNST","RIO","SAP","SIRI","SNY", \
             "SPOT","SRG","SUP","TCI","TH","TJX","TM","TSLA", "TSM","UAN","UL","UNH","UNP","V","VZ","WFC", \
             "WMG","WMT","YTRA"]

stockdata =spark.sql("select * from stocks")
stockdata = stockdata.withColumn("date",to_date(stockdata.date,"yyyy-MM-dd"))

# stocks with closing price datewise
stockscp_df=stockdata.groupBy("date").pivot("symbol", stock_lst).sum("adjClose")
#stockscp_df=stockscp_df.dropna()
stockscp_df.show()

filepath='file:////home/talentum/shared/Project/Project_codes/source/SP500.csv'
stockdata1=spark.read.option("header",True).csv(filepath)
stockdata1 = stockdata1.withColumn("date",to_date(stockdata1.Date,"MM/dd/yyyy"))
stockdata1=stockdata1.dropna()

#SP500 with closing price datewise
sp500_df=stockdata1.select("date","Close")
sp500_df=sp500_df.withColumnRenamed("Close","SP500")
sp500_df=sp500_df.dropna()
sp500_df.show()



#Creating returns Dataframe

from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import functions as F

returns_df_sp500=stockscp_df.join(sp500_df,"date")
returns_df_sp500.show()


my_window = Window.partitionBy().orderBy("date")
for col_name in returns_df_sp500.columns:
    if col_name!='date':
        returns_df_sp500 = returns_df_sp500.withColumn(col_name, F.when(F.isnull(col(col_name) - F.lag(col(col_name)).over(my_window)), 0) \
                                                       .otherwise(col(col_name) - F.lag(col(col_name)).over(my_window))/F.lag(col(col_name)).over(my_window))
for c in returns_df_sp500.columns:
    if c!='date':
        returns_df_sp500 = returns_df_sp500.withColumn(c, F.round(c, 6))

returns_df_sp500=returns_df_sp500.dropna()
returns_df_sp500=returns_df_sp500.drop('date')
returns_df_sp500.show()

returns_df=returns_df_sp500.drop("SP500")
returns_df.show()

from pyspark.mllib.stat import Statistics
import pandas as pd
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

def stocks_correlation(stocks_df,cols):
    # convert to vector column first
    vector_col = "corr_features"
    assembler = VectorAssembler(inputCols=cols, outputCol=vector_col)
    stocks_df_vector = assembler.transform(stocks_df).select(vector_col)

    # get correlation matrix
    matrix = Correlation.corr(stocks_df_vector, vector_col).collect()[0][0]
    corrmatrix = matrix.toArray().tolist()

    corrmatrix_df = spark.createDataFrame(corrmatrix,cols)

    for c in corrmatrix_df.columns:
        corrmatrix_df = corrmatrix_df.withColumn(c,F.round(c,2))

    stock_list = corrmatrix_df.columns #["AAPL","KO","NFLX","DIS","IBM","VZ","WMT","GE","TSLA","MA","AMZN","MSFT","UN","V"]

    # Create a DataFrame from the list of values
    values_df = spark.createDataFrame([Row(new_column=v) for v in stock_list])

    corrmatrix_df = corrmatrix_df.withColumn("row_index", monotonically_increasing_id())
    values_df = values_df.withColumn("row_index", monotonically_increasing_id())

    # Join the DataFrames on the row index
    corr_df = corrmatrix_df.join(values_df, on="row_index").drop("row_index")

    # Show the resulting DataFrame
    corr_df=corr_df.withColumnRenamed("new_column","stockName")
    return corr_df

corr_final_df = stocks_correlation(returns_df,returns_df.columns)
corr_final_df.show()

def highmoderateCorrelation(stock_df,stock_list):
    st_lst = stock_list
    portfolios = []
    for st in st_lst:
        protfolio_st = stock_df[[st,"stockName"]]
        stock_df.createOrReplaceTempView("aapstock")
        query = "select stockName from aapstock where {} between 0.5 and 0.99".format(st) # moderate -0.5 to 0.69 & high 0.7 to 0.99
        portfolio_stocks = spark.sql(query)
        portfolio_i = portfolio_stocks.select("stockName").rdd.flatMap(lambda x:x).collect()
        portfolio_i.append(st)
        portfolios.append(portfolio_i)
    return portfolios

correlated_portfolios = highmoderateCorrelation(corr_final_df,stock_lst)
portfolios = []
for port in correlated_portfolios:
    if 5 < len(port) <= 10:
        portfolios.append(port)

portfolios

port_df = spark.createDataFrame([(row,) for row in portfolios], ["values"])
port_df_exploded = port_df.select(explode(col("values")).alias("value"))
unique_port_df = port_df_exploded.distinct()
unique_port =unique_port_df.rdd.flatMap(lambda x :x).collect()

# Names to Portfolios
names = []
for i in range(1, len(portfolios)+1):
    names.append('Portfolio'+str(i))

from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql.functions import sqrt
from pyspark.mllib.linalg import DenseMatrix

"""
Returns the Annualised Expected Return of a portfolio.
Annualises the return using the 'crude' method.
"""
def getPortReturns():
    portfolio_returns = []
    for port,weight in zip(portfolios,portfolio_weightage):
           
        num_stocks=len(port)
        # creating weight matrix  
        weight_matrix= DenseMatrix(1,num_stocks, weight)
        # Creating Row matrix for the Dense matrix to do dot_product using multiply function
        weight_row_mat = RowMatrix(sc.parallelize(weight_matrix.toArray().tolist()))
        # Extracting mean return of the portfolio
        returns_df_mean_list1=[]
        returns_df_mean_list=[]
        for p in port:
            returns_df_mean_list = returns_df_mean.filter(returns_df_mean.Stocks == p).select("Mean").rdd.flatMap(lambda x: x).collect()
            returns_df_mean_list1.append(returns_df_mean_list[0])
                
        returns_df_matrix=DenseMatrix(len(returns_df_mean_list1),1,returns_df_mean_list1)
        # calculating dot_product of weight and mean return to get the expected return
        exp_ret_portfolio=weight_row_mat.multiply(returns_df_matrix).rows.collect()
        # annual expected return through crude method
        exp_ret_portfolio_annual = exp_ret_portfolio[0][0] * 250
        # Returning the lsit of expected return of portfolios
        portfolio_returns.append(exp_ret_portfolio_annual)
    
    return portfolio_returns

# Calculate the risk of each portfolio
from pyspark.mllib.linalg import DenseMatrix
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql.functions import sqrt
import numpy as np
# arguments are weight and return_df of portfolios #df_temp = return_df[port]
def getPortRisk(weights,df_temp):
    portfolio_risks = []
    
    # calculating the variance covariance matrix of return_df of portfolio
    
    returns_rdd = df_temp.rdd.map(list)
    returns_row_mat = RowMatrix(returns_rdd)
    vcv_matrix=returns_row_mat.computeCovariance()
   
    # Variance of the portfolio = weight matrix dot_product variance-covariance matricx dot_product weight transpose matrix
    weight_matrix= DenseMatrix(1, num_stocks, weights) # weight matrix
    weight_matrix_t= DenseMatrix(num_stocks, 1, weights) # weight matrix Transpose
    weight_row_mat = RowMatrix(sc.parallelize(weight_matrix.toArray().tolist()))
    vcv_w=weight_row_mat.multiply(vcv_matrix) # dot_product of weight matrix and vcv matrix
    var_portfolio = vcv_w.multiply(weight_matrix_t).rows.collect() # dot_product of (weight matrix and vcv matrix in prev line) with weight transpose
    
    # calculating standard deviation from variance and compute annual sd of the portfolio
    var_portfolio=var_portfolio[0][0]
    sd_portfolio=np.sqrt(var_portfolio)
    sd_portfolio_annual = sd_portfolio * np.sqrt(250)
    
    # returning the portfolio risk i.e. standard deviation computed annually
    portfolio_risks.append(sd_portfolio_annual)
    
    return portfolio_risks


returns_df_unique=returns_df[unique_port]

returns_df_unique.show()

# Calculating the weightage of the stocks for the portfolio by minimising risks
from scipy.optimize import minimize
import numpy as np

portfolio_weightage=[]
for port in portfolios:

    num_stocks = len(returns_df_unique[port].columns)  # being the number of stocks (this is a 'global' variable)
    init_weights = [1 / num_stocks] * num_stocks  # initialise weights (x0) [Intially assume equal weightge]
    args=returns_df_unique[port] # second argument of getPortRisk() function
    bounds = tuple((0, 1) for i in range(num_stocks)) # weightage will be between 0 and 1
    cons = ({'type' : 'eq', 'fun' : lambda x : np.sum(x) - 1}) # sum of weightage will be 1
    # getting the optimised weightage through minimise() function with getPortRisk() as objective function and applying bounadries and constraints as adefined above
    results = minimize(fun=getPortRisk, x0=init_weights,args=args, bounds=bounds, constraints=cons)
    # converting the values to float and rounding off to 2 decimal places
    rounded_result = np.round(results.x, decimals=2)
    #print(rounded_result)
    temp_rdd=sc.parallelize(rounded_result)
    temp_rdd=temp_rdd.map(lambda x:float(x))
    
    portfolio_weightage.append(temp_rdd.collect())


# Calculating the investment amount of the portfolio i.e. latest closing price

latest_date = stockscp_df.select(max("date").alias("latest_date")).collect()[0]["latest_date"]
portfolio_amount = []

for port in portfolios:
    amount_rdd=stockscp_df.filter(stockscp_df.date == latest_date).select(port).rdd.flatMap(lambda x: x).map(lambda x: float(x))
    total_amount= amount_rdd.collect()
    portfolio_amount.append(total_amount)


# calculating mean return for each stocks and creating a dtaframe
data1=unique_port
data2=[]
for c in unique_port:
    data2.append(returns_df.select(mean(col(c))).collect()[0][0]) 
data = list(zip(data1, data2))
returns_df_mean=spark.createDataFrame(data,["Stocks","Mean"])
returns_df_mean = returns_df_mean.withColumn("Mean", round(returns_df_mean["Mean"], 6))
returns_df_mean.show()

# function call for returns calulation as per thw portflio weightage
portfolio_return_wt=getPortReturns()

# Calculating beta_value with Linear Regression 

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

assembler = VectorAssembler(inputCols=["SP500"], outputCol="features")
data_with_features = assembler.transform(returns_df_sp500)
betaValues=[]
for col in returns_df_unique.columns:
# Create a LinearRegression model and fit it to the data
    lr = LinearRegression(featuresCol="features", labelCol=col) # passing each stock to LR as labelCol
    model = lr.fit(data_with_features)
    betaValues.append(model.coefficients[0]) # model.coefficients[0] gives beta value of specific stock 

print(betaValues)

#Creating dataframe for Stocks with beta value
d1=unique_port
d2=[float(x) for x in betaValues]
d=zip(d1,d2)
beta_df=spark.createDataFrame(d,["Stocks","Beta"])
beta_df=beta_df.withColumn("Beta",round("Beta",2))
beta_df.show()

# Grouping the beta values for the portfolio

beta_port=[]
for port in portfolios:
    beta_list=[]
    for p in port:
        beta_rdd=beta_df.filter(beta_df.Stocks == p).select("Beta").rdd.flatMap(lambda x: x).map(lambda x: float(x))
        beta_list.append(beta_rdd.collect())
        beta_list_rdd=sc.parallelize(beta_list)
        beta_list_f = beta_list_rdd.flatMap(lambda x: x).collect()
    beta_port.append(beta_list_f)
        
print(beta_port)

# Calculating the final Portfolio Dataframe incorporating Names, Portfolios, Weightage,return,Amount,Beta values

portfolio_return_wt=[float(x) for x in portfolio_return_wt]

data=list(zip(names,portfolios,portfolio_weightage,portfolio_return_wt,portfolio_amount,beta_port))
schema = StructType([
    StructField("Names", StringType(), True),
    StructField("Portfolios", ArrayType(StringType()), True),
    StructField("Weightage", ArrayType(FloatType()), True),
    StructField("TotalReturns", FloatType(), True),
    StructField("Amount", ArrayType(FloatType()), True),
    StructField("BetaPort", ArrayType(FloatType()), True)])
                
final_portfolio_df = spark.createDataFrame(data, schema)
final_portfolio_df.show()

# Calculating the Investment amount and beta value as per the  weightage

final_portfolio_df = final_portfolio_df.withColumn("InvestmentAmount", expr("aggregate(zip_with(Weightage, Amount, (x, y) -> x * y), 0D, (acc, x) -> acc + x)"))
final_portfolio_df=final_portfolio_df.drop("Amount")
final_portfolio_df = final_portfolio_df.withColumn("PortfolioBeta", expr("aggregate(zip_with(Weightage, BetaPort, (x, y) -> x * y), 0D, (acc, x) -> acc + x)"))
final_portfolio_df=final_portfolio_df.drop("BetaPort")
final_portfolio_df = final_portfolio_df.withColumn("TotalReturns", round(final_portfolio_df["TotalReturns"], 2)) \
.withColumn("InvestmentAmount", round(final_portfolio_df["InvestmentAmount"], 2)) \
.withColumn("PortfolioBeta", round(final_portfolio_df["PortfolioBeta"], 2))
final_portfolio_df.show(truncate=False)

# Incorporate Volatility based on beta value , Beta VAlue > 1 High Volatile else Less Volatile

from pyspark.sql import functions as F

final_portfolio_df = final_portfolio_df.withColumn("Volatility", F.when(final_portfolio_df["PortfolioBeta"] > 1, "High") \
                                                   .otherwise("Low"))
final_portfolio_df=final_portfolio_df.drop("PortfolioBeta")
final_portfolio_df.show()
df_exploded1 = final_portfolio_df.select("Names","TotalReturns","InvestmentAmount","Volatility",posexplode("Portfolios").alias("pos", "Portfolios"))
df_exploded2 = final_portfolio_df.select("Names","TotalReturns","InvestmentAmount","Volatility", posexplode("Weightage").alias("pos", "Weightage"))
df_final = df_exploded1.join(df_exploded2, on=["Names","TotalReturns","InvestmentAmount","Volatility", "pos"]) \
.select("Names", "Portfolios", "Weightage","TotalReturns","InvestmentAmount","Volatility")
df_sorted = df_final.sort("Names")
df_sorted.show()


df_sorted_fin=df_sorted.filter(df_sorted.Weightage != 0)

filepath='file:////home/talentum/shared/Project/Project_codes/source/company_names.csv'
company_df=spark.read.option("header",True).csv(filepath)

df_fin=df_sorted_fin.join(company_df,df_sorted_fin.Portfolios==company_df.symbol,"inner")
df_fin=df_fin.drop("symbol")
df_fin.show()

df_fin.write.mode("overwrite").saveAsTable("Portfolios")

portfoliodata=spark.sql("select * from Portfolios")

joineddf = portfoliodata.join(stockdata, portfoliodata["Portfolios"] == stockdata["symbol"], "inner")
joineddf=joineddf.drop("label")
joineddf.show()

joineddf.write.mode("overwrite").saveAsTable("PortfoliosDetail")

filepath='file:////home/talentum/csvfile_final'
joineddf.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(filepath)
