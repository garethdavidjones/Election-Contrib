import matplotlib.pylab as plt
import seaborn as sns
import pandas as pd


output = [('', 796151), ('F', 1426759), ('M', 2900482), ('N', 2982), ('U', 6331)]
#('', 2181964), 


df = pd.DataFrame(output, columns=["Type", "Count"])
df.head()

sns.barplot(df.Type, df.Count)
plt.title("Political Parties")
plt.show()
