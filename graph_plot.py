import matplotlib.pyplot as plt
import pandas as pd

#Indexed or not
index = 0

#Which graph
y_var = "rows_per_second"
#y_var = "ms_per_transaction"

#Read csv and filter the non indexed
data = pd.read_csv("results.csv")
data = data[data.indexed == 0]

#Mean of all the values
mean = data.groupby(["dbms", "batch_size"]).mean().reset_index()

#Standard deviation of all the values
std = data.groupby(["dbms", "batch_size"]).std().reset_index()

#Ploting the lines
for dbMean, dbStd in zip(mean.groupby("dbms"), std.groupby("dbms")):
    print(dbMean)
    print(dbStd)
    x = dbMean[1]["batch_size"]
    y = dbMean[1][y_var]
    y_err = dbStd[1][y_var]
    plt.errorbar(x, y, yerr=y_err, label=dbMean[0], fmt='o-')


#Styling of the plot
plt.grid()
plt.ylabel(y_var, fontsize=12)
plt.xlabel("Batch Size", fontsize=12)
plt.title(f"Average {y_var} for batch sizes 2 to 2000", fontsize=14)
plt.legend(loc="best", title="Database")
plt.savefig(f'Figures/{y_var}.pdf')  
plt.show()