from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import glob

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()


#data_file = glob.glob('/Users/Mehmet/Desktop/dataset/dataset*.csv')


#sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
#print('Total Records = {}'.format(sdfData.count()))
#sdfData.show()

data_file = 'C:/Users/Mehmet/Desktop/dataset/supermarket_dataset.csv'
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
gender = sdfData.groupBy('Gender').count()
print("table gender")
print(gender.show())

#sdfData.registerTemptable("Sales")
sdfData.createOrReplaceTempView("Sales")
 
try:
 output=scSpark.sql('SELECT * FROM Sales')
 print("table1")
 output.show()
except Exception:
 print("output 34 error")   



try:
 output2 = scSpark.sql('SELECT * from sales WHERE `Unit Price` < 15 AND Quantity < 10')
 print("table2")
 output2.show()
except Exception:
 print("output2 error")

try:
 output3 = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
 print("table3")
 output3.show()
except Exception:
  print("output3 error")

try:
 output.write.format('json').mode('overwrite').save('filtered.json')
 print("dosya kaydi")
except Exception as error:
  print("error 53")
  print(error)
  print("error 53")
  
scSpark.stop()
